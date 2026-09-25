//! Portable graph-schema definition DTOs (EH-506).
//!
//! These are data-only descriptions used by clients to exchange a graph schema.
//! They deliberately do not claim that AU's behavior-bearing SchemaPack or
//! registry-node models have the same contract as an engine record.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A named node table and its declared column types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct TableDefinition {
    pub name: String,
    pub columns: BTreeMap<String, String>,
}

/// A relationship type and its allowed endpoint pairs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RelDefinition {
    #[serde(rename = "type")]
    pub relation_type: String,
    pub connections: Vec<BTreeMap<String, String>>,
}

/// Data-only graph-schema definition, with the AU-compatible JSON field names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct GraphSchemaDefinition {
    pub nodes: Vec<TableDefinition>,
    pub edges: Vec<RelDefinition>,
}

#[cfg(test)]
mod tests {
    use super::GraphSchemaDefinition;

    #[test]
    fn au_graph_schema_definition_json_round_trips() {
        let payload = serde_json::json!({
            "nodes": [{"name": "Person", "columns": {"name": "STRING"}}],
            "edges": [{"type": "KNOWS", "connections": [{"from": "Person", "to": "Person"}]}],
        });
        let schema: GraphSchemaDefinition = serde_json::from_value(payload.clone()).unwrap();
        assert_eq!(serde_json::to_value(schema).unwrap(), payload);
    }
}
