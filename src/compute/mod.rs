// CONCEPT:KG-2.22 — Compute Modules
//
// Core compute primitives. `semantic` is always available.
// `spectral`, `hypergraph`, and `distillation` require the `datascience` feature
// with nalgebra/ndarray dependencies — currently disabled in favor of
// pure-Rust implementations in the `datascience` top-level module.

pub mod semantic;

// These modules require nalgebra/ndarray and are gated behind datascience feature.
// They are DEPRECATED in favor of pure-Rust implementations in src/datascience/.
// #[cfg(feature = "datascience")]
// pub mod spectral;
// #[cfg(feature = "datascience")]
// pub mod hypergraph;
// #[cfg(feature = "datascience")]
// pub mod distillation;
