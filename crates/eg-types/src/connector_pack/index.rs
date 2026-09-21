//! What a connector publishes: one index over one archive.
//!
//! A pack is content-addressed end to end. The index names every entry by URI
//! and pins each of its sections as an `(offset, length, sha256)` triple into
//! one archive blob, so the engine can validate a section without reading the
//! rest and can prove the archive it read is the one the index describes.

use serde::{Deserialize, Serialize};

use super::annotations::PackAnnotations;
use crate::contract::{BoundedVec, Digest256, ResourceId};

/// What one pack entry is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PackEntryKind {
    McpServer,
    Tool,
    Skill,
    Prompt,
    // One concrete MCP resource. Its URI is the upstream URI, not an
    // engine-invented `resource://` alias.
    Resource,
    // One MCP resource template. `uri` is the upstream URI template and the
    // input/output sections pin its argument/result schemas.
    ResourceTemplate,
    Ontology,
    Shapes,
    ModelProfile,
    A2aCard,
    Manifest,
}

/// Exact identity of the served MCP catalog snapshot this pack came from.
///
/// The pack digest binds this whole value. Consequently a stored resource can
/// never be mistaken for one observed under a different configuration,
/// connection or authorization scope even when its own body is byte-identical.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct McpCatalogSnapshotBinding {
    pub configuration_revision: u64,
    pub catalog_generation: u64,
    pub snapshot_digest: Digest256,
    pub child_connection_generation: u64,
    pub authorization_scope_digest: Digest256,
}

/// One byte range of the archive, pinned by its own digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackSection {
    pub offset: u64,
    pub length: u64,
    pub sha256: Digest256,
}

/// A reference from one entry to another, by URI and kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackRef {
    pub uri: String,
    pub kind: PackEntryKind,
}

/// One published entry of a pack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackEntry {
    pub kind: PackEntryKind,
    pub uri: String,
    pub name: String,
    pub media_type: String,
    pub body: PackSection,
    #[serde(default)]
    pub input_schema: Option<PackSection>,
    #[serde(default)]
    pub output_schema: Option<PackSection>,
    #[serde(default)]
    pub annotations: PackAnnotations,
    #[serde(default)]
    pub references: BoundedVec<PackRef, 64>,
}

/// Where the archive lives and what it must hash to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackArchiveRef {
    /// The engine blob reference the archive was uploaded under.
    pub blob_digest: String,
    pub length: u64,
    pub sha256: Digest256,
}

/// The tool that produced this pack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackProducer {
    pub name: String,
    pub version: String,
}

/// One connector's complete published surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ConnectorPackIndex {
    pub schema_version: u16,
    pub connector: ResourceId,
    /// The MCP server entry itself. Every other entry hangs off it.
    pub server: PackEntry,
    pub server_package_version: String,
    pub archive: PackArchiveRef,
    pub entries: BoundedVec<PackEntry, 1024>,
    pub producer: PackProducer,
    /// The exact served generation/digest observed by the catalog lister.
    pub catalog: McpCatalogSnapshotBinding,
    /// The digest of everything above, recomputed by the engine before
    /// anything is committed.
    pub pack_digest: Digest256,
}
