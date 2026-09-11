use serde::{Deserialize, Serialize};

use super::selector_ref::{
    AgentLibraryCompositeRef, CanonicalTextAssetRef, GraphTextPropertyRef,
    LakehouseVectorProjectionRef, MultimodalAssetRef, SqlColumnRef, TimeSeriesWindowRef,
    SEMANTIC_SQL_CATALOG_ID, SEMANTIC_SQL_SCHEMA_ID,
};
use super::state::SemanticIndexError;

const MAX_SELECTOR_ID_BYTES: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SemanticSelectorKind {
    SqlColumnRef,
    GraphTextPropertyRef,
    CanonicalTextAsset,
    AgentLibraryComposite,
    MultimodalAssetRef,
    TimeSeriesWindowRef,
    LakehouseVectorProjectionRef,
}

impl SemanticSelectorKind {
    pub fn as_str(self) -> &'static str {
        const NAMES: [&str; 7] = [
            "sql_column_ref",
            "graph_text_property_ref",
            "canonical_text_asset",
            "agent_library_composite",
            "multimodal_asset_ref",
            "time_series_window_ref",
            "lakehouse_vector_projection_ref",
        ];
        NAMES[self as usize]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "selector",
    rename_all = "snake_case",
    deny_unknown_fields
)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SemanticSourceSelector {
    SqlColumnRef(SqlColumnRef),
    GraphTextPropertyRef(GraphTextPropertyRef),
    CanonicalTextAsset(CanonicalTextAssetRef),
    AgentLibraryComposite(AgentLibraryCompositeRef),
    MultimodalAssetRef(MultimodalAssetRef),
    TimeSeriesWindowRef(TimeSeriesWindowRef),
    LakehouseVectorProjectionRef(LakehouseVectorProjectionRef),
}

impl SemanticSourceSelector {
    pub fn kind(&self) -> SemanticSelectorKind {
        match self {
            Self::SqlColumnRef(_) => SemanticSelectorKind::SqlColumnRef,
            Self::GraphTextPropertyRef(_) => SemanticSelectorKind::GraphTextPropertyRef,
            Self::CanonicalTextAsset(_) => SemanticSelectorKind::CanonicalTextAsset,
            Self::AgentLibraryComposite(_) => SemanticSelectorKind::AgentLibraryComposite,
            Self::MultimodalAssetRef(_) => SemanticSelectorKind::MultimodalAssetRef,
            Self::TimeSeriesWindowRef(_) => SemanticSelectorKind::TimeSeriesWindowRef,
            Self::LakehouseVectorProjectionRef(_) => {
                SemanticSelectorKind::LakehouseVectorProjectionRef
            }
        }
    }

    /// Refuse every declared-but-unimplemented selector. There is no string,
    /// expression, or fallback route that can reinterpret it as a SQL column.
    pub fn require_implemented(&self) -> Result<&SqlColumnRef, SemanticIndexError> {
        match self {
            Self::SqlColumnRef(selector) => {
                selector.validate()?;
                Ok(selector)
            }
            _ => Err(SemanticIndexError::UnsupportedSelector {
                selector: self.kind(),
            }),
        }
    }

    pub(crate) fn canonical_cbor(&self) -> Vec<u8> {
        match self {
            Self::SqlColumnRef(value) => value.canonical_cbor(self.kind()),
            Self::GraphTextPropertyRef(value) => value.canonical_cbor(self.kind()),
            Self::CanonicalTextAsset(value) => value.canonical_cbor(self.kind()),
            Self::AgentLibraryComposite(value) => value.canonical_cbor(self.kind()),
            Self::MultimodalAssetRef(value) => value.canonical_cbor(self.kind()),
            Self::TimeSeriesWindowRef(value) => value.canonical_cbor(self.kind()),
            Self::LakehouseVectorProjectionRef(value) => value.canonical_cbor(self.kind()),
        }
    }
}

impl SqlColumnRef {
    fn validate(&self) -> Result<(), SemanticIndexError> {
        for (field, value) in [
            ("catalog_id", &self.catalog_id),
            ("schema_id", &self.schema_id),
            ("table_id", &self.table_id),
            ("column_id", &self.column_id),
        ] {
            validate_id(field, value)?;
        }
        if self.catalog_id != SEMANTIC_SQL_CATALOG_ID || self.schema_id != SEMANTIC_SQL_SCHEMA_ID {
            return Err(SemanticIndexError::InvalidField {
                field: "sql_column_ref".to_string(),
                reason: format!(
                    "current SQL authority requires catalog={SEMANTIC_SQL_CATALOG_ID} and schema={SEMANTIC_SQL_SCHEMA_ID}"
                ),
            });
        }
        Ok(())
    }
}

fn validate_id(field: &str, value: &str) -> Result<(), SemanticIndexError> {
    if value.is_empty()
        || value.len() > MAX_SELECTOR_ID_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(SemanticIndexError::InvalidField {
            field: field.to_string(),
            reason: "must be non-empty, bounded, and preserve exact visible bytes".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_column_is_the_only_implemented_selector() {
        let sql = SemanticSourceSelector::SqlColumnRef(SqlColumnRef {
            catalog_id: SEMANTIC_SQL_CATALOG_ID.into(),
            schema_id: SEMANTIC_SQL_SCHEMA_ID.into(),
            table_id: "table".into(),
            column_id: "body".into(),
        });
        assert!(sql.require_implemented().is_ok());

        let unsupported = [
            SemanticSourceSelector::GraphTextPropertyRef(GraphTextPropertyRef {
                graph_id: "graph".into(),
                type_id: "document".into(),
                property_id: "body".into(),
            }),
            SemanticSourceSelector::CanonicalTextAsset(CanonicalTextAssetRef {
                asset_id: "asset".into(),
                asset_revision: "1".into(),
                content_digest: "digest".into(),
            }),
            SemanticSourceSelector::AgentLibraryComposite(AgentLibraryCompositeRef {
                agent_id: "agent".into(),
                role_digest: "role".into(),
                system_prompt_digest: "prompt".into(),
                tool_set_digest: "tools".into(),
                skill_set_digest: "skills".into(),
                model_profile_digest: "model".into(),
                ontology_set_digest: "ontology".into(),
            }),
            SemanticSourceSelector::MultimodalAssetRef(MultimodalAssetRef {
                asset_id: "asset".into(),
                media_kind: "image".into(),
                asset_revision: "1".into(),
                content_digest: "digest".into(),
            }),
            SemanticSourceSelector::TimeSeriesWindowRef(TimeSeriesWindowRef {
                series_id: "series".into(),
                window_start: "start".into(),
                window_end: "end".into(),
                source_revision: "1".into(),
            }),
            SemanticSourceSelector::LakehouseVectorProjectionRef(LakehouseVectorProjectionRef {
                catalog_id: "catalog".into(),
                namespace_id: "namespace".into(),
                table_id: "table".into(),
                projection_id: "projection".into(),
                source_revision: "1".into(),
            }),
        ];
        for selector in unsupported {
            assert_eq!(
                selector.require_implemented(),
                Err(SemanticIndexError::UnsupportedSelector {
                    selector: selector.kind(),
                })
            );
        }

        let wrong_catalog = SemanticSourceSelector::SqlColumnRef(SqlColumnRef {
            catalog_id: "shared".into(),
            schema_id: SEMANTIC_SQL_SCHEMA_ID.into(),
            table_id: "articles".into(),
            column_id: "body".into(),
        });
        assert!(matches!(
            wrong_catalog.require_implemented(),
            Err(SemanticIndexError::InvalidField { .. })
        ));
    }

    #[test]
    fn sql_selector_has_no_expression_or_predicate_field() {
        let encoded = serde_json::json!({
            "kind": "sql_column_ref",
            "selector": {
                "catalog_id": "catalog",
                "schema_id": "schema",
                "table_id": "table",
                "column_id": "body",
                "raw_expression": "body || secret"
            }
        });
        assert!(serde_json::from_value::<SemanticSourceSelector>(encoded).is_err());
    }
}
