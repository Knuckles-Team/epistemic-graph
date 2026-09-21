//! What each connector-pack operation answers with.
//!
//! An import has three outcomes and they are all RESULTS, not errors: it
//! landed, it was already the head, or it was rejected with every violation
//! named. Only a write-time conflict -- a stale head, a missing body, a
//! reserved id, an idempotency clash -- is an error response, because those
//! say the caller should retry or look elsewhere rather than fix the pack.

use serde::{Deserialize, Serialize};

use crate::connector_pack::McpCatalogSnapshotBinding;
use crate::contract::{closed_error_codes, BoundedVec, Digest256, ResourceId};

/// Why a pack was rejected. Screaming-snake on the wire, so the code a caller
/// reads is the code the design documents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PackViolationCode {
    MalformedIndex,
    UnknownEntryKind,
    PackTooLarge,
    ArchiveMissing,
    ArchiveDigestMismatch,
    MalformedSections,
    PackDigestMismatch,
    DuplicateComponentId,
    ForbiddenEntryKind,
    MalformedBody,
    MissingToolSchema,
    UnknownCapabilityIri,
    InvalidAnnotation,
    InvalidFacts,
    OntologyInvalid,
    ShapesInvalid,
    OntologyInconsistent,
    ValidationBudgetExceeded,
    ShaclViolation,
    UnresolvedReference,
    ReferenceCycle,
    InvalidComponent,
    ImporterMismatch,
    PackMassWithdrawal,
    RetiredEntryReturned,
}

/// Something worth saying that did not stop the import.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PackWarningCode {
    EmptyDescription,
    UnresolvedCapabilityIri,
    DuplicateShapeIri,
}

/// One rejection, attributed to the entry that caused it where possible.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackViolation {
    pub code: PackViolationCode,
    #[serde(default)]
    pub uri: Option<String>,
    /// At most one kibibyte, so a rejection cannot become an exfiltration
    /// channel for the pack's own content.
    pub detail: String,
}

/// One warning, in the same shape as a violation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackWarning {
    pub code: PackWarningCode,
    #[serde(default)]
    pub uri: Option<String>,
    pub detail: String,
}

/// What an import did to each entry, counted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackDispositionCounts {
    pub published: u32,
    pub revised: u32,
    pub unchanged: u32,
    pub withdrawn: u32,
    pub republished: u32,
}

/// Where the graph projection of this head stands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "projection", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PackProjectionState {
    /// Nothing to project.
    None,
    /// Queued on the import outbox.
    Pending,
    Applied {
        graph: String,
        graph_version: u64,
    },
    Failed {
        code: String,
    },
}

/// The durable receipt of one landed import.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackImportReceipt {
    pub schema_version: u16,
    pub tenant_id: String,
    pub connector: ResourceId,
    pub binding_revision: u64,
    pub pack_digest: Digest256,
    pub catalog: McpCatalogSnapshotBinding,
    #[serde(default)]
    pub previous_pack_digest: Option<Digest256>,
    pub record_id: String,
    pub batch_id: String,
    pub committed_version: u64,
    pub counts: PackDispositionCounts,
    pub warnings: BoundedVec<PackWarning, 256>,
    pub projection: PackProjectionState,
}

/// What one import attempt produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PackImportResult {
    Imported {
        receipt: PackImportReceipt,
    },
    /// Byte-identical to the current head; nothing was written.
    Unchanged {
        pack_digest: Digest256,
        binding_revision: u64,
    },
    /// Validation refused the pack. Nothing was written, and every reason is
    /// named; `budget_exhausted` says the list is a prefix, not the whole set.
    Rejected {
        #[serde(default)]
        pack_digest: Option<Digest256>,
        violations: BoundedVec<PackViolation, 256>,
        budget_exhausted: bool,
    },
}

/// The current head, as `status` reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackHeadView {
    pub binding_revision: u64,
    pub pack_digest: Digest256,
    pub catalog: McpCatalogSnapshotBinding,
    #[serde(default)]
    pub server_contract_version: Option<String>,
    pub server_package_version: String,
    pub record_id: String,
    /// The record id the graph projection currently exposes, when it lags the
    /// committed one.
    #[serde(default)]
    pub visible_record_id: Option<String>,
    pub committed_at_ms: u64,
}

/// How many members the connector has, by lifecycle.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackMemberCounts {
    pub published: u32,
    pub withdrawn: u32,
    pub retired: u32,
}

/// One connector's pack state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ConnectorPackStatus {
    pub schema_version: u16,
    pub tenant_id: String,
    pub connector: ResourceId,
    #[serde(default)]
    pub head: Option<PackHeadView>,
    pub members: PackMemberCounts,
    #[serde(default)]
    pub last_receipt: Option<PackImportReceipt>,
    pub warnings: BoundedVec<PackWarning, 256>,
    /// Who may publish this connector's packs. Served only to pack-control and
    /// administrative callers: to everyone else it is another tenant's
    /// operational detail.
    #[serde(default)]
    pub importer: Option<String>,
    pub projection: PackProjectionState,
}

/// What a bind or unbind committed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ConnectorPackBindingResult {
    pub schema_version: u16,
    pub tenant_id: String,
    pub connector: ResourceId,
    #[serde(default)]
    pub importer: Option<String>,
    pub bound_by: String,
    pub bound_at_ms: u64,
    pub batch_id: String,
    pub committed_version: u64,
}

/// What a retire committed, entry by entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackRetireResult {
    pub schema_version: u16,
    pub tenant_id: String,
    pub connector: ResourceId,
    pub retired: BoundedVec<crate::agent_component::ComponentDependency, 1024>,
    pub batch_id: String,
    pub committed_version: u64,
}

/// What one body-reconciliation sweep found and released.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackBodyReconcileReport {
    pub schema_version: u16,
    pub tenant_id: String,
    pub scanned: u64,
    pub orphaned: u64,
    pub released: u64,
    /// When the next sweep may usefully run. A sweep that is called back
    /// sooner does the same scan for nothing.
    pub next_run_after_ms: u64,
}

closed_error_codes! {
    /// Write-time refusals. Unlike a [`PackViolation`] these are not about the
    /// pack's content, so they travel as error responses rather than as part
    /// of a result a caller would otherwise store.
    pub enum PackWriteErrorCode {
        /// The head moved between the caller's read and its import.
        PackHeadConflict => "PACK_HEAD_CONFLICT",
        /// The projection plan the import was built against is stale.
        PackPlanStale => "PACK_PLAN_STALE",
        /// A section the index pins is not in the archive store.
        BodyMissing => "BODY_MISSING",
        /// The pack names a component id the engine owns.
        ReservedComponentId => "RESERVED_COMPONENT_ID",
        /// The same idempotency key was reused for different content.
        IdempotencyConflict => "IDEMPOTENCY_CONFLICT",
    }
}
