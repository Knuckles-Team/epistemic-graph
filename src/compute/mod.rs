// CONCEPT:KG-2.22 — Compute Modules
//
// Core compute primitives. `semantic` is always available.
// `spectral`, `hypergraph`, and `distillation` require the `datascience` feature
// with nalgebra/ndarray dependencies — currently disabled in favor of
// pure-Rust implementations in the `datascience` top-level module.

pub mod semantic;

pub mod hypergraph;
