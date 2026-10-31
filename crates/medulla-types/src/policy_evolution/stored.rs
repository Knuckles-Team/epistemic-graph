//! The one row shape a policy-evolution record is stored as, and the internal
//! write that is the ONLY way to create it.
//!
//! `PolicyEvolution` admits a record against the request graph and then asks
//! the durable WorkItem kernel to store it through the internal
//! `PolicyEvolutionStore` method. Generic graph writes may not create, change or
//! remove a row of this shape (the engine's row guard refuses them), so a
//! stored record can only have passed admission. Reads still re-derive the id
//! as defence in depth.

use serde::{Deserialize, Serialize};

use super::{PolicyEvolutionRecord, PolicyRefusal};

/// `node_type` of every policy-evolution record row.
pub const POLICY_RECORD_NODE_TYPE: &str = "PolicyEvolutionRecord";

/// Whether a stored or incoming node property map is a policy-evolution row.
pub fn is_policy_evolution_row(row: &serde_json::Map<String, serde_json::Value>) -> bool {
    row.get("node_type").and_then(serde_json::Value::as_str) == Some(POLICY_RECORD_NODE_TYPE)
}

/// One admitted record, stamped by the engine from the verified request
/// context. Carried by the internal `PolicyEvolutionStore` method so the
/// kernel write -- and its raft replay -- is fully determined by the method.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct StoredPolicyRecord {
    pub record_id: String,
    pub tenant_id: String,
    pub recorded_by: String,
    pub recorded_at_ms: u64,
    pub record: PolicyEvolutionRecord,
}

impl StoredPolicyRecord {
    /// The record's id must be the content address of its tenant and body.
    pub fn verify_identity(&self) -> Result<(), PolicyRefusal> {
        match self.record.record_id(&self.tenant_id) {
            Ok(derived) if derived == self.record_id => Ok(()),
            _ => Err(PolicyRefusal::RecordTampered(self.record_id.clone())),
        }
    }

    /// The node property map the kernel stores.
    pub fn row(&self) -> Result<serde_json::Map<String, serde_json::Value>, String> {
        let mut row = match serde_json::to_value(self) {
            Ok(serde_json::Value::Object(row)) => row,
            _ => return Err("policy record encoding failed".to_string()),
        };
        row.insert("node_type".into(), POLICY_RECORD_NODE_TYPE.into());
        Ok(row)
    }

    /// Decode a stored row back into the stamped record, ignoring the
    /// kernel's own bookkeeping fields. `None` for any other row.
    pub fn from_row(row: &serde_json::Map<String, serde_json::Value>) -> Option<Self> {
        if !is_policy_evolution_row(row) {
            return None;
        }
        let fields = [
            "record_id",
            "tenant_id",
            "recorded_by",
            "recorded_at_ms",
            "record",
        ];
        let own: serde_json::Map<String, serde_json::Value> = fields
            .iter()
            .filter_map(|key| row.get(*key).map(|value| (key.to_string(), value.clone())))
            .collect();
        serde_json::from_value(serde_json::Value::Object(own)).ok()
    }
}

/// The kernel's answer to one `PolicyEvolutionStore`: `created == false` means
/// the content-addressed row already existed. `changed_work_item_ids` names the
/// rows the serving projection must republish.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PolicyRecordStored {
    pub created: bool,
    pub changed_work_item_ids: Vec<String>,
}
