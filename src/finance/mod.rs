// CONCEPT:KG-2.20 — Rust-Native Finance Compute Suite
//
// Feature-gated financial compute primitives served over the Tokio service
// protocol (MessagePack/UDS), not an in-process Python extension.
// Replaces Python scipy/statsmodels/hmmlearn for hot-path finance ops.

// Numeric linear-algebra/optimization code (Kalman, simplex, covariance, regime
// transition matrices) reads more clearly — and is less error-prone to maintain —
// with explicit `for i in 0..n { m[i][j] }` indexing than with enumerate/zip
// rewrites. Scope the lint to this compute module rather than contorting the math.
#![allow(clippy::needless_range_loop)]

pub mod derivatives;
pub mod exchange;
pub mod forensic;
pub mod optimizer;
pub mod quant;
pub mod regime;
pub mod risk;
pub mod signals;
pub mod statespace;
