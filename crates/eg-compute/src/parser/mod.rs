// CONCEPT:EG-KG.compute.repository-parser — Repository Parser Module
//
// Feature-gated tree-sitter integration for source code parsing.
// Requires the `ast` feature flag.

#[cfg(feature = "ast")]
pub mod tree_sitter;

// CONCEPT:EG-KG.compute.turn-each-project — cross-file call/import resolution over a parsed batch.
#[cfg(feature = "ast")]
pub mod resolve;

// CONCEPT:EH-280 — branch-aware, blob-deduplicated indexing: every unique blob is
// parsed once and projected as `:Blob` / `:FileVersion` / `:Branch` membership.
#[cfg(feature = "ast")]
pub mod branch_index;
#[cfg(feature = "ast")]
mod branch_projection;
#[cfg(feature = "ast")]
mod branch_scope;
#[cfg(feature = "ast")]
pub mod enrichment_admission;
/// The action a repository enrichment budget top-up must be verified for. The
/// durable store checks it in every build, so it lives outside the `ast` gate.
pub const TOP_UP_ACTION: &str = "repository:enrichment:budget:top_up";
#[cfg(feature = "ast")]
pub mod enrichment_reactivation;
#[cfg(feature = "ast")]
pub mod enrichment_snapshot;
