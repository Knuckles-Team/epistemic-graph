// CONCEPT:EG-KG.compute.repository-parser — Repository Parser Module
//
// Feature-gated tree-sitter integration for source code parsing.
// Requires the `ast` feature flag.

#[cfg(feature = "ast")]
pub mod tree_sitter;

// CONCEPT:EG-KG.compute.turn-each-project — cross-file call/import resolution over a parsed batch.
#[cfg(feature = "ast")]
pub mod resolve;
