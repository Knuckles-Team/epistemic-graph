//! The durable record of one import, and the payload of its outbox topic.
//!
//! The receipt tells the caller what happened; this record is what the engine
//! keeps, so a question asked months later -- which pack revision introduced
//! this tool, and who imported it -- is answerable from the store rather than
//! from a log.

use serde::{Deserialize, Serialize};

use super::index::{McpCatalogSnapshotBinding, PackEntryKind, PackProducer};
use super::result::{PackProjectionState, PackWarning};
use crate::agent_component::ComponentDependency;
use crate::contract::{BoundedVec, Digest256, ResourceId};

/// What an import did to one entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PackDisposition {
    Published,
    Revised,
    Unchanged,
    Withdrawn,
    Republished,
}

/// The MCP server this pack served, as the record keeps it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackServerRecord {
    pub name: String,
    #[serde(default)]
    pub contract_version: Option<String>,
    pub package_version: String,
    pub component: ComponentDependency,
}

/// The archive an import read, pinned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackArchiveFacts {
    pub length: u64,
    pub sha256: Digest256,
}

/// One entry's fate, and the component revision it became.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackEntryRecord {
    pub uri: String,
    pub kind: PackEntryKind,
    pub entry_digest: Digest256,
    pub disposition: PackDisposition,
    pub component_id: String,
    pub entry_revision: u64,
    pub definition_digest: String,
}

/// One durable import record. Also the payload of the import topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackImportRecord {
    pub schema_version: u16,
    pub tenant_id: String,
    pub connector: ResourceId,
    pub binding_revision: u64,
    pub record_id: String,
    pub pack_digest: Digest256,
    pub catalog: McpCatalogSnapshotBinding,
    #[serde(default)]
    pub previous_pack_digest: Option<Digest256>,
    pub server: PackServerRecord,
    pub producer: PackProducer,
    pub archive: PackArchiveFacts,
    pub importer: String,
    pub committed_at_ms: u64,
    pub entries: BoundedVec<PackEntryRecord, 1024>,
    pub warnings: BoundedVec<PackWarning, 256>,
    pub projection: PackProjectionState,
}
