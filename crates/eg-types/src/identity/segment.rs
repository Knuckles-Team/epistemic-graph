//! Principal-owned knowledge units with explicit sharing scope.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// The closed sharing scope of one knowledge unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ShareScope {
    Private,
    Placement,
    Tenant,
}

/// One versioned unit of principal-owned knowledge.
///
/// The visibility behavior is a separate criterion; this model only carries
/// the identity, lineage and explicit sharing declaration that behavior reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct KnowledgeUnit {
    pub id: String,
    pub owner: String,
    pub version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<String>,
    #[serde(default)]
    pub access_labels: BTreeSet<String>,
    pub share_scope: ShareScope,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Native qualification is intentionally supplied by the managed lane; the
    // exact proving test remains unbound until that run passes.
    #[test]
    fn unit_round_trips() {
        let unit = KnowledgeUnit {
            id: "knowledge:unit-1".to_string(),
            owner: "usr:alice".to_string(),
            version: 3,
            supersedes: Some("knowledge:unit-0".to_string()),
            access_labels: BTreeSet::from(["sensitive".to_string(), "research".to_string()]),
            share_scope: ShareScope::Placement,
        };
        let encoded = serde_json::to_vec(&unit).expect("knowledge unit serializes");
        let decoded: KnowledgeUnit =
            serde_json::from_slice(&encoded).expect("knowledge unit deserializes");
        assert_eq!(decoded, unit);

        let mut unknown_scope = serde_json::to_value(&unit).expect("knowledge unit is JSON");
        unknown_scope["share_scope"] = serde_json::Value::String("org".to_string());
        assert!(serde_json::from_value::<KnowledgeUnit>(unknown_scope).is_err());
    }
}
