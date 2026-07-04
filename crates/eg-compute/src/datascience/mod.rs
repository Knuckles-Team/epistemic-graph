// CONCEPT:EG-KG.compute.rust-native-training-loss — Rust-Native Data Science Primitives
//
// Feature-gated ML primitives that replace sklearn for compute-heavy operations.
// Served over the Tokio service protocol for consumption by data-science-mcp.

// Numeric matrix code (covariance, Cholesky/back-substitution, k-means centroids,
// simplex) is clearer and safer with explicit index loops than enumerate/zip
// rewrites. Scope the lint here rather than contorting the linear algebra.
#![allow(clippy::needless_range_loop)]

pub mod estimators;
pub mod primitives;
pub mod training;
