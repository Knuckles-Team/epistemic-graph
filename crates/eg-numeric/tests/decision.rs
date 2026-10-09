//! Integration tests of the statistical decision layer (`--features decision`):
//! features, the act/abstain ladder, keyed exploration, label admission,
//! fitting, evaluation, NL slot filling and the resident scorer (EH-291..303),
//! including the §11.2 negative fixtures. Shared fixtures live in [`common`].
#![cfg(feature = "decision")]

#[path = "decision/admission.rs"]
mod admission;
#[path = "decision/common.rs"]
mod common;
#[path = "decision/drift.rs"]
mod drift;
#[path = "decision/features.rs"]
mod features;
#[path = "decision/fit_eval.rs"]
mod fit_eval;
#[path = "decision/head_kinds.rs"]
mod head_kinds;
#[path = "decision/ladder.rs"]
mod ladder;
#[path = "decision/nl.rs"]
mod nl;
#[path = "decision/scorer.rs"]
mod scorer;
#[path = "decision/scorer_promotion.rs"]
mod scorer_promotion;
