use serde::{Deserialize, Serialize};

use super::digest::cbor;
use super::SemanticSelectorKind;

pub const SEMANTIC_SQL_CATALOG_ID: &str = "tenant";
pub const SEMANTIC_SQL_SCHEMA_ID: &str = "public";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlColumnRef {
    pub catalog_id: String,
    pub schema_id: String,
    pub table_id: String,
    pub column_id: String,
}

impl SqlColumnRef {
    pub(crate) fn canonical_cbor(&self, kind: SemanticSelectorKind) -> Vec<u8> {
        cbor::map([
            ("kind", cbor::text(kind.as_str())),
            ("catalog_id", cbor::text(&self.catalog_id)),
            ("schema_id", cbor::text(&self.schema_id)),
            ("table_id", cbor::text(&self.table_id)),
            ("column_id", cbor::text(&self.column_id)),
        ])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphTextPropertyRef {
    pub graph_id: String,
    pub type_id: String,
    pub property_id: String,
}

impl GraphTextPropertyRef {
    pub(crate) fn canonical_cbor(&self, kind: SemanticSelectorKind) -> Vec<u8> {
        cbor::map([
            ("kind", cbor::text(kind.as_str())),
            ("graph_id", cbor::text(&self.graph_id)),
            ("type_id", cbor::text(&self.type_id)),
            ("property_id", cbor::text(&self.property_id)),
        ])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalTextAssetRef {
    pub asset_id: String,
    pub asset_revision: String,
    pub content_digest: String,
}

impl CanonicalTextAssetRef {
    pub(crate) fn canonical_cbor(&self, kind: SemanticSelectorKind) -> Vec<u8> {
        cbor::map([
            ("kind", cbor::text(kind.as_str())),
            ("asset_id", cbor::text(&self.asset_id)),
            ("asset_revision", cbor::text(&self.asset_revision)),
            ("content_digest", cbor::text(&self.content_digest)),
        ])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentLibraryCompositeRef {
    pub agent_id: String,
    pub role_digest: String,
    pub system_prompt_digest: String,
    pub tool_set_digest: String,
    pub skill_set_digest: String,
    pub model_profile_digest: String,
    pub ontology_set_digest: String,
}

impl AgentLibraryCompositeRef {
    pub(crate) fn canonical_cbor(&self, kind: SemanticSelectorKind) -> Vec<u8> {
        cbor::map([
            ("kind", cbor::text(kind.as_str())),
            ("agent_id", cbor::text(&self.agent_id)),
            ("role_digest", cbor::text(&self.role_digest)),
            (
                "system_prompt_digest",
                cbor::text(&self.system_prompt_digest),
            ),
            ("tool_set_digest", cbor::text(&self.tool_set_digest)),
            ("skill_set_digest", cbor::text(&self.skill_set_digest)),
            (
                "model_profile_digest",
                cbor::text(&self.model_profile_digest),
            ),
            ("ontology_set_digest", cbor::text(&self.ontology_set_digest)),
        ])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultimodalAssetRef {
    pub asset_id: String,
    pub media_kind: String,
    pub asset_revision: String,
    pub content_digest: String,
}

impl MultimodalAssetRef {
    pub(crate) fn canonical_cbor(&self, kind: SemanticSelectorKind) -> Vec<u8> {
        cbor::map([
            ("kind", cbor::text(kind.as_str())),
            ("asset_id", cbor::text(&self.asset_id)),
            ("media_kind", cbor::text(&self.media_kind)),
            ("asset_revision", cbor::text(&self.asset_revision)),
            ("content_digest", cbor::text(&self.content_digest)),
        ])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeSeriesWindowRef {
    pub series_id: String,
    pub window_start: String,
    pub window_end: String,
    pub source_revision: String,
}

impl TimeSeriesWindowRef {
    pub(crate) fn canonical_cbor(&self, kind: SemanticSelectorKind) -> Vec<u8> {
        cbor::map([
            ("kind", cbor::text(kind.as_str())),
            ("series_id", cbor::text(&self.series_id)),
            ("window_start", cbor::text(&self.window_start)),
            ("window_end", cbor::text(&self.window_end)),
            ("source_revision", cbor::text(&self.source_revision)),
        ])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LakehouseVectorProjectionRef {
    pub catalog_id: String,
    pub namespace_id: String,
    pub table_id: String,
    pub projection_id: String,
    pub source_revision: String,
}

impl LakehouseVectorProjectionRef {
    pub(crate) fn canonical_cbor(&self, kind: SemanticSelectorKind) -> Vec<u8> {
        cbor::map([
            ("kind", cbor::text(kind.as_str())),
            ("catalog_id", cbor::text(&self.catalog_id)),
            ("namespace_id", cbor::text(&self.namespace_id)),
            ("table_id", cbor::text(&self.table_id)),
            ("projection_id", cbor::text(&self.projection_id)),
            ("source_revision", cbor::text(&self.source_revision)),
        ])
    }
}
