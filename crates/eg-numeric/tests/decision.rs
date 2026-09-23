//! Integration tests of the statistical decision layer (`--features decision`):
//! features, the act/abstain ladder, keyed exploration, label admission,
//! fitting, evaluation and NL slot filling, including the §11.2 negative
//! fixtures. Shared fixtures live in [`common`].
#![cfg(feature = "decision")]

#[path = "decision/admission.rs"]
mod admission;
#[path = "decision/common.rs"]
mod common;
#[path = "decision/features.rs"]
mod features;
#[path = "decision/fit_eval.rs"]
mod fit_eval;
#[path = "decision/ladder.rs"]
mod ladder;
#[path = "decision/nl.rs"]
mod nl;
