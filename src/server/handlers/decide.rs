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

pub(crate) use assemble::handle_agent_assemble;
pub(crate) use commit::handle_decision_commit;
pub(crate) use jobs::{handle_decision_eval, handle_decision_fit};
pub(crate) use statistical::handle_decide;
