//! Keyed schema sources on one request graph (X9).
//!
//! A graph's shapes and ontology stop being one anonymous blob and become a
//! keyed SET of sources, each with an origin. That is what makes a connector
//! pack's shapes and an operator's shapes composable without either one being
//! able to silently erase the other: attaching under a key replaces that key
//! and nothing else, and the composed digest says which set is in force.
//!
//! `pack:` and `ingest:` are reserved key prefixes. A source under one of them
//! is owned by an importer, so an operator attaching there by hand would be
//! forging the provenance the key asserts.

use serde::{Deserialize, Serialize};

use crate::contract::{closed_error_codes, BoundedVec, ResourceId};

/// Format identity (RF-ADR-006) of the schema-source views.
pub const GRAPH_SCHEMA_RESULT_SCHEMA_VERSION: u16 = 1;

/// Most schema sources one graph may carry.
pub const MAX_GRAPH_SCHEMA_SOURCES: usize = 32;
/// Largest immutable engine core catalog returned by list.
pub const MAX_CORE_GRAPH_SCHEMA_SOURCES: usize = 32;
/// Largest single shapes or ontology document.
pub const MAX_SCHEMA_DOCUMENT_BYTES: usize = 2 << 20;
/// Longest schema source key.
pub const MAX_SCHEMA_SOURCE_ID_BYTES: usize = 128;

/// Key prefixes only an importer may write.
pub const RESERVED_SCHEMA_SOURCE_PREFIXES: &[&str] = &["core:", "pack:", "ingest:"];

/// Attach, replace or detach one keyed schema source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum GraphSchemaOp {
    /// Attach or replace the documents under `source_id`. At least one
    /// document must be given; attaching nothing is a detach, and saying so
    /// explicitly is what stops an empty request from silently clearing a key.
    Attach {
        source_id: String,
        #[serde(default)]
        shapes_ttl: Option<String>,
        #[serde(default)]
        ontology_ttl: Option<String>,
        /// Compare-and-set against the composed digest the caller last saw.
        #[serde(default)]
        if_composed_digest: Option<String>,
    },
    /// Attach the current head of a connector's pack, under its reserved key.
    AttachPack {
        connector: ResourceId,
        #[serde(default)]
        if_composed_digest: Option<String>,
    },
    Detach {
        source_id: String,
        #[serde(default)]
        if_composed_digest: Option<String>,
    },
}

impl GraphSchemaOp {
    /// Every operation writes: even a detach that finds nothing has to be
    /// ordered against concurrent attaches.
    pub fn is_mutation(&self) -> bool {
        true
    }

    /// The audit token naming what was done.
    pub fn action_token(&self) -> &'static str {
        match self {
            Self::Attach { .. } => "attach",
            Self::AttachPack { .. } => "attach_pack",
            Self::Detach { .. } => "detach",
        }
    }

    /// The audit token naming the source key it was done to.
    pub fn source_token(&self) -> &str {
        match self {
            Self::Attach { source_id, .. } | Self::Detach { source_id, .. } => source_id,
            Self::AttachPack { connector, .. } => connector.as_str(),
        }
    }

    /// The composed digest this operation is fenced on, if any.
    pub fn expected_composed_digest(&self) -> Option<&str> {
        match self {
            Self::Attach {
                if_composed_digest, ..
            }
            | Self::AttachPack {
                if_composed_digest, ..
            }
            | Self::Detach {
                if_composed_digest, ..
            } => if_composed_digest.as_deref(),
        }
    }

    /// Bounds and the reserved-prefix rule.
    pub fn validate(&self) -> Result<(), String> {
        validate_expected_composed_digest(self)?;
        validate_operation_payload(self)
    }
}

fn validate_expected_composed_digest(operation: &GraphSchemaOp) -> Result<(), String> {
    let Some(expected) = operation.expected_composed_digest() else {
        return Ok(());
    };
    crate::contract::Digest256::parse(expected)
        .map(|_| ())
        .map_err(|error| format!("invalid if_composed_digest: {error}"))
}

fn validate_operation_payload(operation: &GraphSchemaOp) -> Result<(), String> {
    match operation {
        GraphSchemaOp::Attach {
            source_id,
            shapes_ttl,
            ontology_ttl,
            ..
        } => {
            validate_source_id(source_id)?;
            validate_document("shapes_ttl", shapes_ttl.as_deref())?;
            validate_document("ontology_ttl", ontology_ttl.as_deref())?;
            if shapes_ttl.is_none() && ontology_ttl.is_none() {
                return Err("graph schema attach needs at least one document".to_string());
            }
            Ok(())
        }
        GraphSchemaOp::AttachPack { .. } => Ok(()),
        GraphSchemaOp::Detach { source_id, .. } => validate_source_id(source_id),
    }
}

/// Whether `source_id` is owned by an importer rather than an operator.
pub fn is_reserved_schema_source(source_id: &str) -> bool {
    RESERVED_SCHEMA_SOURCE_PREFIXES
        .iter()
        .any(|prefix| source_id.starts_with(prefix))
}

fn validate_source_id(source_id: &str) -> Result<(), String> {
    if source_id.is_empty() || source_id.len() > MAX_SCHEMA_SOURCE_ID_BYTES {
        return Err(format!(
            "graph schema source id must be 1..={MAX_SCHEMA_SOURCE_ID_BYTES} bytes"
        ));
    }
    if source_id.chars().any(char::is_control) {
        return Err("graph schema source id carries a control character".to_string());
    }
    validate_source_namespace(source_id)
}

fn validate_source_namespace(source_id: &str) -> Result<(), String> {
    if source_id == "operator" || is_reserved_schema_source(source_id) {
        return Err(format!(
            "{}: '{source_id}' is owned by an importer",
            GraphSchemaErrorCode::SourceReserved.as_str()
        ));
    }
    if source_id.strip_prefix("admin:").is_none_or(str::is_empty) {
        return Err("graph schema source id must use the admin:<name> namespace".to_string());
    }
    Ok(())
}

fn validate_document(field: &str, document: Option<&str>) -> Result<(), String> {
    match document {
        None => Ok(()),
        Some(document) if document.len() <= MAX_SCHEMA_DOCUMENT_BYTES => Ok(()),
        Some(_) => Err(format!(
            "graph schema {field} exceeds {MAX_SCHEMA_DOCUMENT_BYTES} bytes"
        )),
    }
}

/// Who attached one source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "origin", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SchemaSourceOriginView {
    Core {
        module: String,
        version: u32,
        set_digest: String,
    },
    /// The pre-X9 integrity policy, lifted into a keyed source.
    Operator,
    Admin {
        name: String,
    },
    Pack {
        connector: String,
        record_id: String,
    },
    Ingestion {
        mapping: String,
        revision: u64,
    },
}

/// One attached source, without its documents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GraphSchemaSourceView {
    pub source_id: String,
    pub origin: SchemaSourceOriginView,
    #[serde(default)]
    pub shapes_sha256: Option<String>,
    #[serde(default)]
    pub ontology_sha256: Option<String>,
    pub shapes_bytes: u64,
    pub ontology_bytes: u64,
    pub attached_at_ms: u64,
}

/// What one attach or detach committed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GraphSchemaCommitted {
    pub schema_version: u16,
    pub graph: String,
    pub composed_digest: String,
    pub graph_version: u64,
    /// False when the request was a no-op against the current set.
    pub changed: bool,
}

/// The graph's whole schema-source set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GraphSchemaSourcesView {
    pub schema_version: u16,
    pub graph: String,
    /// Identity of the binary-owned catalog, independent of graph attachments.
    pub core_catalog_digest: String,
    /// Identity of the exact core+dynamic composition.
    pub composed_digest: String,
    pub core_sources: BoundedVec<GraphSchemaSourceView, MAX_CORE_GRAPH_SCHEMA_SOURCES>,
    pub dynamic_sources: BoundedVec<GraphSchemaSourceView, MAX_GRAPH_SCHEMA_SOURCES>,
}

closed_error_codes! {
    /// Every typed refusal the schema-source surface can answer with.
    pub enum GraphSchemaErrorCode {
        /// The key is under a reserved importer prefix.
        SourceReserved => "SCHEMA_SOURCE_RESERVED",
        /// Two sources declare the same shape or term incompatibly.
        SourceConflict => "SCHEMA_SOURCE_CONFLICT",
        ShapesInvalid => "SHAPES_INVALID",
        OntologyInvalid => "ONTOLOGY_INVALID",
        /// A logical OWL construct is outside the certified native profile.
        OwlUnsupportedConstruct => "OWL_UNSUPPORTED_CONSTRUCT",
        /// The composed ontology has no model.
        OntologyInconsistent => "ONTOLOGY_INCONSISTENT",
        ValidationBudgetExceeded => "VALIDATION_BUDGET_EXCEEDED",
        /// The attach would make the graph's existing data invalid.
        SourceRegression => "SCHEMA_SOURCE_REGRESSION",
        SourcesTooLarge => "SCHEMA_SOURCES_TOO_LARGE",
        /// The compare-and-set digest did not match.
        ComposedDigestMismatch => "COMPOSED_DIGEST_MISMATCH",
        /// The engine has no snapshot-bound connector-pack body resolver.
        AttachPackResolverUnavailable => "ATTACH_PACK_RESOLVER_UNAVAILABLE",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attach(source_id: &str, shapes_ttl: Option<String>) -> GraphSchemaOp {
        GraphSchemaOp::Attach {
            source_id: source_id.to_string(),
            shapes_ttl,
            ontology_ttl: None,
            if_composed_digest: None,
        }
    }

    #[test]
    fn operator_and_importer_owned_keys_are_not_generic_attach_targets() {
        for source_id in ["operator", "core:x@1", "pack:x", "ingest:x"] {
            let error = attach(source_id, Some("valid".to_string()))
                .validate()
                .unwrap_err();
            assert!(error.contains(GraphSchemaErrorCode::SourceReserved.as_str()));
        }
    }

    #[test]
    fn generic_attach_requires_the_admin_namespace() {
        let error = attach("free-form", Some("valid".to_string()))
            .validate()
            .unwrap_err();
        assert!(error.contains("admin:<name>"));
        attach("admin:local", Some("valid".to_string()))
            .validate()
            .unwrap();
    }

    #[test]
    fn a_document_one_byte_over_the_bound_is_refused() {
        let document = "x".repeat(MAX_SCHEMA_DOCUMENT_BYTES + 1);
        let error = attach("admin:oversized", Some(document))
            .validate()
            .unwrap_err();
        assert!(error.contains(&MAX_SCHEMA_DOCUMENT_BYTES.to_string()));
    }
}
