//! The statistical decision layer's engine-side computation (DL-2/DL-4/DL-6/DL-8).
//!
//! Everything a `Decide`, `DecisionFit` or `DecisionEval` answer depends on is
//! computed here, over the wire types in `eg_types::decision`, on top of the
//! pinned deterministic kernels in [`crate::detkernel`]. Living inside this
//! crate is deliberate: the crate's `clippy.toml` bans every platform float
//! transcendental, so the ban covers the decision path by construction rather
//! than by a second policy file that could drift.
//!
//! Module DAG (acyclic, depth <= 3): `quant`/`refusal` are leaves; `bm25`,
//! `candidate`, `exploration` and `admission` sit on them; `features` and
//! `head_eval` read those; `ladder`, `fit`, `evaluate` and `nl` are the tops.
//!
//! Nothing here reads a clock, a hash map's iteration order, or a thread pool:
//! reductions are serial over sorted keys, ties break by option order (the
//! caller sorts options by id), and every value that reaches a record is
//! quantised first.

pub mod admission;
pub mod bm25;
pub mod candidate;
pub mod evaluate;
mod evaluate_bandit;
pub mod exploration;
pub mod features;
pub mod fit;
mod fit_calibrate;
pub mod head_eval;
pub mod ladder;
pub mod nl;
pub mod quant;
pub mod refusal;

pub use refusal::{Refusal, RefusalResult};
