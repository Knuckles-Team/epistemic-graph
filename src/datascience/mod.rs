// CONCEPT:KG-2.22 — Rust-Native Data Science Primitives
//
// Feature-gated ML primitives that replace sklearn for compute-heavy operations.
// Served over the Tokio service protocol for consumption by data-science-mcp.

pub mod estimators;
pub mod primitives;
