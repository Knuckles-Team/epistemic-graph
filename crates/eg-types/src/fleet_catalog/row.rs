//! What the fleet catalog projection returns.
//!
//! Every content row is a VIEW of one published `AgentComponent` revision --
//! it carries that revision's `entry_revision` and `definition_digest`, so a
//! caller can always get back to the authoritative record -- joined with the
//! discovery observation that made it visible and any operator override.

use serde::{Deserialize, Serialize};

use super::request::{DiscoveryCounts, FleetCatalogCursor};
use super::vocabulary::{
    DiscoveryOutcome, DiscoveryScope, FleetCatalogKind, FleetVisibility, FleetWriteDisposition,
    ResourceKind, SkillType, SkillTypeSource, ToolMode,
};
use super::{MAX_FLEET_LOOKUP_IDS, MAX_FLEET_PAGE_ROWS};
use crate::agent_component::ToolEffect;
use crate::contract::{BoundedVec, Digest256, ResourceId};

/// Who may see a row, and who published what it shows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FleetRowAcl {
    pub tenant_id: String,
    pub visibility: FleetVisibility,
    /// The opaque persistence id of the principal that wrote the underlying
    /// record: the observer for a discovery row, the importer for a component.
    pub publisher: String,
}

/// One server's latest observation under one scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FleetDiscoveryRow {
    /// `srvobs:<server_name>:<scope key>` -- stable across revisions.
    pub id: String,
    pub server_name: String,
    pub scope: DiscoveryScope,
    pub connector: ResourceId,
    pub outcome: DiscoveryOutcome,
    pub counts: DiscoveryCounts,
    /// Server clock, never the caller's.
    pub observed_at_ms: u64,
    pub revision: u64,
    pub acl: FleetRowAcl,
}

/// The part of every content row that identifies its component revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FleetComponentRef {
    /// The `AgentComponent` id (`mcp:<connector>/<kind>/<name>`).
    pub id: String,
    /// The name the server itself uses.
    pub name: String,
    pub description: String,
    pub server_name: String,
    pub connector: ResourceId,
    /// Published, and its server is not registered as disabled.
    pub enabled: bool,
    pub entry_revision: u64,
    pub definition_digest: String,
    pub acl: FleetRowAcl,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FleetToolRow {
    pub component: FleetComponentRef,
    /// `sha256:<hex>` of the served input schema, when the pack pinned one.
    pub input_schema_digest: Option<String>,
    pub effect: ToolEffect,
    pub tool_mode: ToolMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FleetPromptRow {
    pub component: FleetComponentRef,
    pub uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FleetResourceRow {
    pub component: FleetComponentRef,
    pub uri: String,
    pub media_type: Option<String>,
    pub resource_kind: ResourceKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FleetSkillRow {
    pub component: FleetComponentRef,
    pub uri: String,
    pub skill_type: SkillType,
    /// `skill_type`'s display label, computed by the engine.
    pub classification: String,
    pub skill_type_source: SkillTypeSource,
    /// The override record's revision, when an override decided `skill_type`.
    pub override_revision: Option<u64>,
}

/// One row of any kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FleetCatalogRow {
    Discovery { row: FleetDiscoveryRow },
    Tool { row: FleetToolRow },
    Prompt { row: FleetPromptRow },
    Resource { row: FleetResourceRow },
    Skill { row: FleetSkillRow },
}

/// What one row is ABOUT: an observation, or a component revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FleetRowSubject<'a> {
    Observation(&'a FleetDiscoveryRow),
    Component(&'a FleetComponentRef),
}

impl FleetCatalogRow {
    pub fn subject(&self) -> FleetRowSubject<'_> {
        match self {
            Self::Discovery { row } => FleetRowSubject::Observation(row),
            Self::Tool { row } => FleetRowSubject::Component(&row.component),
            Self::Prompt { row } => FleetRowSubject::Component(&row.component),
            Self::Resource { row } => FleetRowSubject::Component(&row.component),
            Self::Skill { row } => FleetRowSubject::Component(&row.component),
        }
    }

    /// `(name, id)`: what rows are ordered and searched by, and addressed by.
    pub fn key(&self) -> (&str, &str) {
        match self.subject() {
            FleetRowSubject::Observation(row) => (&row.server_name, &row.id),
            FleetRowSubject::Component(component) => (&component.name, &component.id),
        }
    }
}

/// One bounded page of one kind, fenced to the snapshot it was cut from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FleetCatalogPage {
    pub schema_version: u16,
    pub kind: FleetCatalogKind,
    pub rows: BoundedVec<FleetCatalogRow, MAX_FLEET_PAGE_ROWS>,
    /// Every visible row matching the request's filter, across all pages.
    pub total: u32,
    pub next_cursor: Option<FleetCatalogCursor>,
    pub commons_revision: u64,
    pub snapshot_digest: Digest256,
    pub observed_at_ms: u64,
}

/// The visible rows for a lookup, in request order. Unknown and invisible ids
/// are both simply absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FleetCatalogLookup {
    pub schema_version: u16,
    pub rows: BoundedVec<FleetCatalogRow, MAX_FLEET_LOOKUP_IDS>,
    pub commons_revision: u64,
    pub observed_at_ms: u64,
}

/// What one fleet catalog write committed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FleetWriteReceipt {
    /// The logical record id (a discovery id, or `ovr:<field>:<component id>`).
    pub record_id: String,
    /// The record's revision after this write.
    pub revision: u64,
    pub disposition: FleetWriteDisposition,
    pub observed_at_ms: u64,
}
