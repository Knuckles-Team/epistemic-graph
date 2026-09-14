//! Wire result DTOs for visualization operations.

use serde::{Deserialize, Serialize};

use crate::viz::VizFormat;

#[cfg(feature = "viz")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct VizCapabilityMatrix {
    pub entries: Vec<VizCapabilityEntry>,
}

#[cfg(feature = "viz")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct VizCapabilityEntry {
    pub mark: String,
    pub surface: String,
    pub level: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_wave: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

#[cfg(feature = "viz")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct VizPayloadRef {
    pub id: String,
    pub kind: String,
    pub byte_len: u64,
}

#[cfg(feature = "viz")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct VizViewResult {
    pub query_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
    pub row_count: u64,
    pub lod_tier: String,
    pub reduction: String,
    pub exact: bool,
    pub payloads: Vec<VizPayloadRef>,
    pub wall_time_ms: u64,
    pub produced_at_unix_ms: i64,
}

#[cfg(feature = "viz")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct VizRenderResponse {
    pub view_result: VizViewResult,
    pub format: VizFormat,
    pub content_type: String,
    pub result_ref: String,
    pub cached: bool,
    #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
}

#[cfg(feature = "viz")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct VizProvenanceRecord {
    pub result_ref: String,
    pub query_hash: String,
    pub dataset_ref: String,
    pub content_fingerprint: u64,
    pub algo_family: String,
    pub algo_name: String,
    pub lod_tier: String,
    pub exact: bool,
    pub row_count: u64,
    pub width_px: u32,
    pub height_px: u32,
    pub format: String,
    pub wall_time_ms: u64,
    pub produced_at_unix_ms: i64,
}

#[cfg(all(test, feature = "viz"))]
mod tests {
    use super::*;

    #[test]
    fn optional_fields_follow_viz_wire_omission_contract() {
        let entry = VizCapabilityEntry {
            mark: "line".to_string(),
            surface: "static_export".to_string(),
            level: "full".to_string(),
            status: "shipped".to_string(),
            target_wave: None,
            notes: None,
        };
        let entry_json = serde_json::to_value(&entry).expect("serialize capability entry");
        assert!(!entry_json.as_object().unwrap().contains_key("target_wave"));
        assert!(!entry_json.as_object().unwrap().contains_key("notes"));

        let result = VizViewResult {
            query_hash: "qh".to_string(),
            seed: None,
            row_count: 0,
            lod_tier: "direct".to_string(),
            reduction: "none".to_string(),
            exact: true,
            payloads: Vec::new(),
            wall_time_ms: 0,
            produced_at_unix_ms: 0,
        };
        let result_json = serde_json::to_value(&result).expect("serialize view result");
        assert!(!result_json.as_object().unwrap().contains_key("seed"));
    }

    #[test]
    fn optional_fields_remain_present_when_populated() {
        let entry = VizCapabilityEntry {
            mark: "line".to_string(),
            surface: "static_export".to_string(),
            level: "full".to_string(),
            status: "planned".to_string(),
            target_wave: Some("V2".to_string()),
            notes: Some("planned support".to_string()),
        };
        let entry_json = serde_json::to_value(&entry).expect("serialize capability entry");
        assert_eq!(entry_json["target_wave"].as_str(), Some("V2"));
        assert_eq!(entry_json["notes"].as_str(), Some("planned support"));

        let result = VizViewResult {
            query_hash: "qh".to_string(),
            seed: Some(42),
            row_count: 1,
            lod_tier: "tiled".to_string(),
            reduction: "tiled".to_string(),
            exact: false,
            payloads: Vec::new(),
            wall_time_ms: 1,
            produced_at_unix_ms: 1,
        };
        let result_json = serde_json::to_value(&result).expect("serialize view result");
        assert_eq!(result_json["seed"].as_u64(), Some(42));
    }
}
