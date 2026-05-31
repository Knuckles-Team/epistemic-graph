// CONCEPT:KG-2.20 — Rust-Native Finance Compute Suite
//
// Feature-gated financial compute primitives exposed via PyO3.
// Replaces Python scipy/statsmodels/hmmlearn for hot-path finance ops.

pub mod optimizer;
pub mod risk;
pub mod regime;
pub mod signals;
pub mod exchange;
