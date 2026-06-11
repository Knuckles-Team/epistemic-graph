// CONCEPT:KG-2.20 — Rust-Native Finance Compute Suite
//
// Feature-gated financial compute primitives served over the Tokio service
// protocol (MessagePack/UDS), not an in-process Python extension.
// Replaces Python scipy/statsmodels/hmmlearn for hot-path finance ops.

pub mod derivatives;
pub mod exchange;
pub mod forensic;
pub mod optimizer;
pub mod quant;
pub mod regime;
pub mod risk;
pub mod signals;
pub mod statespace;
