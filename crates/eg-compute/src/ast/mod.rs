// CONCEPT:EG-KG.compute.ast-code-analysis — AST Code Analysis Module
//
// Feature-gated tree-sitter integration for multi-language AST parsing.
// Emits Symbol nodes for KG persistence via the mutation ledger.

pub mod symbol;

#[cfg(feature = "ast")]
pub mod parser;
