//! Rendered skill package content derived from the current graph revision.

use serde::{Deserialize, Serialize};

/// A read-time skill rendering tied to the graph and content it used.
///
/// The catalog projection owns the rendering policy. This type only carries
/// the identity and provenance needed for a reader to tell which graph
/// revision produced the package and which ontology references participated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RenderedSkillPackage {
    /// The skill URI represented by this package.
    pub uri: String,
    /// Digest of the rendered package content.
    pub digest: String,
    /// Ontology references used while rendering, in projection order.
    pub ontology_refs: Vec<String>,
    /// Graph revision from which the content and references were read.
    pub graph_revision: u64,
}
