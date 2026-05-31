// CONCEPT:KG-2.21 — Repository Parser Module
//
// Feature-gated tree-sitter integration for source code parsing.
// Requires the `ast` feature flag.

#[cfg(feature = "ast")]
pub mod tree_sitter;
