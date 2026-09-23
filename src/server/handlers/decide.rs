//! The Decide layer's served surface (RF-ADR-010).
//!
//! Four methods, four files: assembly, its commit, the statistical executor
//! and the two admin jobs. Each entry point's signature is frozen by the
//! contract wave; the package that lands a handler replaces only its body.

pub(crate) mod assemble;
pub(crate) mod commit;
pub(crate) mod jobs;
#[cfg(feature = "decide")]
pub(crate) mod shape;
pub(crate) mod statistical;

// The statistical package (EH-056 ... EH-074): candidate sources, the
// decision executor, the NL template binding and the two admin jobs.
#[cfg(feature = "decide")]
mod candidates;
#[cfg(feature = "decide")]
mod stat_decide;
#[cfg(feature = "decide")]
mod stat_executor;
#[cfg(feature = "decide")]
mod stat_jobs;
#[cfg(feature = "decide")]
mod stat_nl;
#[cfg(feature = "decide")]
mod stat_support;
#[cfg(all(test, feature = "decide"))]
mod stat_tests;
#[cfg(feature = "decide")]
mod telemetry;

pub(crate) use assemble::handle_agent_assemble;
pub(crate) use commit::handle_decision_commit;
pub(crate) use jobs::{handle_decision_eval, handle_decision_fit};
pub(crate) use statistical::handle_decide;
