// CONCEPT:KG-2.22 — Compute Modules
//
// Core compute primitives. `semantic` is always available (no heavy deps).
// `hypergraph` carries the nalgebra/rand_chacha/rand_distr dependency, so it is
// gated behind the `datascience` feature — a slim `--features server` build omits
// it (and those deps) entirely. Its one wire method is deprecated; keep it gated
// rather than always-on so the lean build doesn't link linear-algebra crates.

pub mod semantic;

#[cfg(feature = "datascience")]
pub mod hypergraph;
