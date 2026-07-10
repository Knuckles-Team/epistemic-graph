// CONCEPT:EG-KG.compute.ast-code-analysis — AST Code Analysis Module
//
// Feature-gated tree-sitter integration for multi-language AST parsing.
// Emits Symbol nodes for KG persistence via the mutation ledger.

pub mod symbol;

#[cfg(feature = "ast")]
pub mod parser;

// CONCEPT:EG-KG.compute depth (per-modality): call-graph + module dependency-graph
// + change-causality, built over `crate::parser::resolve::IndexResult`'s resolved
// `calls`/`depends_on` edges. Gated behind `ast` (the SAME feature `parser::resolve`
// itself needs) — pure adjacency + BFS, no new deps.
#[cfg(feature = "ast")]
pub mod graph;
