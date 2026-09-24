//! Reading policy-evolution records from the request graph.
//!
//! One row per record, keyed by the record's content-addressed id and written
//! only by the WorkItem kernel's `PolicyEvolutionStore` (the row guard refuses
//! generic writes). Decoding still re-derives the id from the stored body as
//! defence in depth: a row whose body no longer hashes to its id is refused as
//! tampered.

use eg_types::policy_evolution::{
    is_policy_evolution_row, PolicyEvolutionRecord, PolicyRecordView, PolicyRefusal,
    StoredPolicyRecord,
};

use super::gate::RecordLookup;
use crate::graph::GraphCore;

/// The `type` marker `StartTrajectory` stamps on a trajectory node.
const TRAJECTORY_NODE_TYPE: &str = "Trajectory";

/// Decode one row as `tenant_id`'s record `record_id`.
///
/// `Ok(None)` for anything that is not this tenant's policy record; a
/// policy-record row whose body is undecodable or no longer hashes to its id
/// is [`PolicyRefusal::RecordTampered`].
pub(super) fn decode_view(
    record_id: &str,
    properties_msgpack: &[u8],
    tenant_id: &str,
) -> Result<Option<PolicyRecordView>, PolicyRefusal> {
    let tampered = || PolicyRefusal::RecordTampered(record_id.to_string());
    let Ok(serde_json::Value::Object(row)) =
        eg_types::msgpack::decode_property_value(properties_msgpack)
    else {
        return Ok(None);
    };
    if !is_policy_evolution_row(&row) {
        return Ok(None);
    }
    let stored = StoredPolicyRecord::from_row(&row).ok_or_else(tampered)?;
    if stored.tenant_id != tenant_id {
        return Ok(None);
    }
    if stored.record_id != record_id || stored.verify_identity().is_err() {
        return Err(tampered());
    }
    Ok(Some(PolicyRecordView {
        record_id: stored.record_id,
        recorded_by: stored.recorded_by,
        recorded_at_ms: stored.recorded_at_ms,
        record: stored.record,
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

    /// The row the kernel would store for `record` under tenant-a.
    fn row_blob(record: &PolicyEvolutionRecord) -> (String, Vec<u8>) {
        let stored = StoredPolicyRecord {
            record_id: record.record_id("tenant-a").unwrap(),
            tenant_id: "tenant-a".to_string(),
            recorded_by: "principal:sha256:ab".to_string(),
            recorded_at_ms: 42,
            record: record.clone(),
        };
        let row = serde_json::Value::Object(stored.row().unwrap());
        (stored.record_id, rmp_serde::to_vec_named(&row).unwrap())
    }

    #[test]
    fn a_record_round_trips_through_its_node_and_stays_tenant_scoped() {
        let record = version();
        let (id, blob) = row_blob(&record);
        let view = decode_view(&id, &blob, "tenant-a").unwrap().unwrap();
        assert_eq!(view.record, record);
        assert_eq!(view.recorded_at_ms, 42);
        assert_eq!(decode_view(&id, &blob, "tenant-b"), Ok(None));
    }

    #[test]
    fn an_edited_record_is_refused_as_tampered() {
        let (id, blob) = row_blob(&version());
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
