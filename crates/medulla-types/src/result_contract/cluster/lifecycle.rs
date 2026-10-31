//! Graph lifecycle and service health result bodies of the `cluster` domain.

use serde::{Deserialize, Serialize};

use crate::protocol::GraphType;

/// `CreateGraph`: the created graph's name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GraphCreated {
    pub created: String,
}

/// `DeleteGraph`: the deleted graph's name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GraphDeleted {
    pub deleted: String,
}

/// How much of a graph incarnation is materialized in memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum MaterializationPhase {
    CatalogOnly,
    Partial,
    Complete,
    Failed,
}

/// Row offsets a partial materialization has reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MaterializationCursor {
    pub node_offset: u64,
    pub edge_offset: u64,
}

/// Source rows a maintained index covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct IndexCompleteness {
    pub nodes: u64,
    pub edges: u64,
    pub complete: bool,
}

/// One server-maintained index of a listed graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct IndexManifestListing {
    /// `label`, `property`, `ontology`, `vector`, `text`, ...
    pub kind: String,
    pub source_snapshot_version: u64,
    pub build_version: u32,
    pub completeness_cursor: IndexCompleteness,
    /// `building`, `valid`, `stale` or `failed`.
    pub validity: String,
}

/// One graph the caller may read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GraphListing {
    pub name: String,
    #[serde(rename = "type")]
    pub graph_type: GraphType,
    pub materialization: Option<MaterializationPhase>,
    pub source_snapshot_version: Option<u64>,
    pub completeness_cursor: Option<MaterializationCursor>,
    pub valid: bool,
    pub index_manifests: Vec<IndexManifestListing>,
}

/// Graph materialization counts reported by `Health`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GraphLifecycleHealth {
    pub catalog_graphs: u64,
    pub resident_graphs: u64,
    pub complete_graphs: u64,
    pub partial_graphs: u64,
    pub failed_graphs: u64,
    pub all_resident_materializations_valid: bool,
}

/// `Health`: liveness plus the served-operation set clients negotiate against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HealthReport {
    pub status: String,
    pub uptime_s: u64,
    pub mem_bytes: u64,
    pub version: String,
    pub graph_lifecycle: GraphLifecycleHealth,
    pub ops: Vec<String>,
}
