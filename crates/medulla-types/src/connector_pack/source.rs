//! Held live-source claims extracted from an imported pack's ontology.

use serde::{Deserialize, Serialize};

/// A connector declaration never supplies admission approval.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PackSourceApproval {
    #[default]
    Held,
    Approved,
}

/// Access kinds retain their SDK contract spelling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PackSourceKind {
    McpTool,
    HttpEndpoint,
    GraphqlQuery,
    A2aSkill,
    VirtualGraph,
}

impl PackSourceKind {
    /// The corresponding existing federation wire kind; this grants no driver.
    pub fn foreign_kind(self) -> &'static str {
        match self {
            Self::McpTool => "mcp",
            Self::HttpEndpoint => "api",
            Self::GraphqlQuery => "graphql",
            Self::A2aSkill => "a2a",
            Self::VirtualGraph => "named",
        }
    }
}

/// One source declaration, pinned by its containing member's body digest.
/// Import always records `Held`; a publisher cannot approve its own mapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackSourceRegistration {
    pub connector: super::super::contract::ResourceId,
    pub contract_iri: String,
    pub resource_uri: String,
    pub source_kind: PackSourceKind,
    #[serde(default)]
    pub approval: PackSourceApproval,
    pub class_iri: String,
    pub source_id: String,
    pub operation: String,
    pub entity: String,
    pub key_field: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: MEDULLA-FEDERATED-QUERY-R722.1
    #[test]
    fn access_kinds_map_to_foreign_source_kinds() {
        for (kind, expected) in [
            (PackSourceKind::McpTool, "mcp"),
            (PackSourceKind::HttpEndpoint, "api"),
            (PackSourceKind::GraphqlQuery, "graphql"),
            (PackSourceKind::A2aSkill, "a2a"),
            (PackSourceKind::VirtualGraph, "named"),
        ] {
            assert_eq!(kind.foreign_kind(), expected);
            let encoded = serde_json::to_vec(&kind).unwrap();
            assert_eq!(
                serde_json::from_slice::<PackSourceKind>(&encoded).unwrap(),
                kind
            );
        }
    }
}
