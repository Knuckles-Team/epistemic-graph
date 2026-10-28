//! Pure data for an architecture view derived from graph evidence.
//!
//! The derivation and served read path live above `eg-types`.  These structs
//! deliberately carry only the stable view contract: deployable containers,
//! supporting source paths, and call edges.

use serde::{Deserialize, Serialize};

/// A graph-derived architecture view.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ArchitectureView {
    /// Deployable containers present in the view.
    pub containers: Vec<ViewContainer>,
    /// Cross-container calls represented by the view.
    pub edges: Vec<ViewEdge>,
}

/// One deployable container and the ingested source paths that support it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ViewContainer {
    /// Stable display/name key for the container.
    pub name: String,
    /// Technology label associated with the container.
    pub technology: String,
    /// Source paths whose ingested evidence supports the container.
    pub support: Vec<String>,
}

/// A directed call edge between two architecture containers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ViewEdge {
    /// Source container name.
    pub from: String,
    /// Destination container name.
    pub to: String,
    /// Kind of relationship represented by the edge.
    pub kind: String,
    /// Number of supporting calls for this relationship.
    pub call_count: u64,
}
