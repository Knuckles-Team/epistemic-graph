//! Bounded 0-1 integer programming solver: planted optima, exhaustive
//! cross-checks, adversarial budgets, infeasibility, certificate tampering and
//! determinism. Test modules live under `tests/solve/`; this file only loads
//! them so cargo builds one test binary.

#![cfg(feature = "solve")]

#[path = "solve/adversarial.rs"]
mod adversarial;
#[path = "solve/determinism.rs"]
mod determinism;
#[path = "solve/exhaustive.rs"]
mod exhaustive;
#[path = "solve/generators.rs"]
mod generators;
#[path = "solve/model_validation.rs"]
mod model_validation;
#[path = "solve/planted.rs"]
mod planted;
#[path = "solve/support.rs"]
mod support;
#[path = "solve/tamper_proof.rs"]
mod tamper_proof;
#[path = "solve/tamper_status.rs"]
mod tamper_status;
