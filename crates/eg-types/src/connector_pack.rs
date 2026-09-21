//! Connector MCP pack import: the wire contract (RF-ADR-009).
//!
//! A connector publishes its whole served surface as ONE pack -- an index over
//! one content-addressed archive -- and the engine imports it atomically. That
//! is the point: a partially-imported connector is a connector whose tools and
//! the schemas they were reviewed against disagree, and nothing downstream can
//! tell which half it is looking at.
//!
//! Three properties hold throughout:
//!
//! * **Content decides identity.** Entry, annotation and pack digests are pure
//!   functions of what the connector published, so re-packing identical
//!   content is a no-op rather than a new revision.
//! * **A declaration is a claim.** Costs, latencies and capability IRIs are
//!   recorded with their provenance, never promoted to measurements.
//! * **Disappearing is reversible; retiring is not.** An entry missing from a
//!   later pack is WITHDRAWN and can come back; permanent removal is a
//!   separate, administrative operation.

pub mod annotations;
pub mod digest;
pub mod ids;
pub mod index;
pub mod ops;
pub mod record;
pub mod result;

/// Format identity (RF-ADR-006) of [`index::ConnectorPackIndex`].
pub const CONNECTOR_PACK_SCHEMA_VERSION: u16 = 2;
/// Format identity of [`record::PackImportRecord`].
pub const PACK_IMPORT_RECORD_SCHEMA_VERSION: u16 = 2;

/// Most entries one pack may carry.
pub const MAX_PACK_ENTRIES: usize = 1024;
/// Most outgoing references one entry may declare.
pub const MAX_PACK_REFERENCES: usize = 64;
/// Most IRIs one annotation list may carry.
pub const MAX_PACK_IRI_ITEMS: usize = 64;
/// Most violations one rejection may name.
pub const MAX_PACK_VIOLATIONS: usize = 256;
/// Largest encoded index the engine will read.
pub const MAX_PACK_INDEX_BYTES: usize = 1 << 20;
/// Largest archive one pack may reference.
pub const MAX_PACK_ARCHIVE_BYTES: u64 = 16 << 20;
/// Largest single entry body.
pub const MAX_PACK_BODY_BYTES: u64 = 2 << 20;
/// Largest single schema section.
pub const MAX_PACK_SCHEMA_SECTION_BYTES: u64 = 1 << 20;
/// Largest encoded import record.
pub const MAX_PACK_RECORD_BYTES: usize = 4 << 20;

/// The outbox topic one committed import publishes on.
pub const CONNECTOR_PACK_IMPORT_TOPIC: &str = "eg.connector-pack.import.v1";
/// Typed mutation-result schema carried by a committed import receipt.
pub const CONNECTOR_PACK_RESULT_SCHEMA_ID: &str = "connector-pack-import-result.v1";

/// Closed mapping selected from a connector pack's authoritative manifest.
/// Ingestion consumes this type and never parses caller-supplied manifest text.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ConnectorSchemaMapping {
    pub ontology_class: String,
    pub fields: std::collections::BTreeMap<String, String>,
}

pub use annotations::{PackAnnotations, PackCost, PackModelFacts};
pub use ids::{escape_pack_name, pack_component_id, PACK_COMPONENT_ID_PREFIX};
pub use index::{
    ConnectorPackIndex, McpCatalogSnapshotBinding, PackArchiveRef, PackEntry, PackEntryKind,
    PackProducer, PackRef, PackSection,
};
pub use ops::{
    ConnectorPackBindRequest, ConnectorPackImportRequest, ConnectorPackOp,
    ConnectorPackReconcileRequest, ConnectorPackReprojectRequest, ConnectorPackRetireRequest,
    ConnectorPackStatusRequest, ConnectorPackUnbindRequest, PackHeadRef,
};
pub use record::{
    PackArchiveFacts, PackDisposition, PackEntryRecord, PackImportRecord, PackServerRecord,
};
pub use result::{
    ConnectorPackBindingResult, ConnectorPackStatus, PackBodyReconcileReport,
    PackDispositionCounts, PackHeadView, PackImportReceipt, PackImportResult, PackMemberCounts,
    PackProjectionState, PackRetireResult, PackViolation, PackViolationCode, PackWarning,
    PackWarningCode, PackWriteErrorCode,
};
