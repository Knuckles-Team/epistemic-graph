//! `eg-quantum-core` — Quantum as a governed epistemic-graph capability, Lane Q0
//! contracts (register `D-QN-1`).
//!
//! CONCEPT:EG-KG.compute.quantum-circuit-ir — the stable `QuantumProgram` IR, the
//! vendor-neutral `QuantumBackend` trait + capability flags, the deterministic
//! planner (rules R0-R5), and the exactness-typed `QuantumResult` that keeps
//! sampled/noisy/hardware results out of the epistemic hard-constraint path by
//! construction. This is the one contract every other quantum crate in this
//! workspace (`eg-quantum-gates`, `eg-quantum-sim`) builds against.
//!
//! This crate is the CONTRACT for quantum-as-a-capability: a stable IR
//! ([`ir::QuantumProgram`]), a vendor-neutral execution trait with capability flags
//! ([`backend::QuantumBackend`]), a pure planner rule engine
//! ([`planner::select_backend`]) over a pure circuit analyzer
//! ([`estimate::estimate`]), and an exactness-typed result
//! ([`result::QuantumResult`]) that keeps sampled/noisy/hardware results out of the
//! epistemic hard-constraint path by construction, not by convention.
//!
//! **Q0 explicitly ships no simulator, no vendored SDK, and no numeric kernel.**
//! Getting this shape right — so Q1 (vendor QuantRS2), Q3 (QuEST backend), Q4
//! (pure-Rust backends), and everything after can build against a stable surface —
//! matters more than anything here actually running yet.
//!
//! # Numeric-stack decision (the Q0 charge to decide and record this now)
//!
//! **Decision: the quantum numeric path BRIDGES TO `eg-numeric`. It does not, and
//! must not, carry its own numeric stack.**
//!
//! This crate itself needs no numeric library at all — `estimate()`'s memory
//! projections (`statevector_bytes`/`density_matrix_bytes` in `estimate.rs`) are
//! `2^n`/`4^n` integer arithmetic, not linear algebra, because Q0 has no simulator to
//! feed real amplitudes through. The decision below is about what Q1/Q3/Q4 (which DO
//! need complex statevectors, density matrices, and unitary gate application) must
//! target, confirmed against the actual code in both repositories rather than
//! asserted from the charter alone:
//!
//! - **`eg-numeric`** (`crates/eg-numeric` in this workspace) is the engine's one
//!   existing numeric kernel: `ndarray` 0.17 + `nalgebra` 0.35, BLAS/LAPACK-free,
//!   real-valued today (no `Complex` usage in its `src/` at the time of this
//!   decision). Critically, it is NOT starting from zero for complex support: the
//!   workspace `Cargo.lock` shows `nalgebra` already depends on `num-complex 0.4.6`
//!   transitively (nalgebra needs `ComplexField` for its own generic linear algebra).
//!   That means adding a `Complex64` array/matrix surface to `eg-numeric` — which Q1
//!   will need for statevectors/density matrices/unitaries — adds **zero new crates**
//!   to the workspace's resolved dependency graph. It is a small extension of an
//!   already-present, already-vetted stack, not a new one.
//! - **QuantRS2** (`open-source-libraries/quantrs`, the Q1 vendoring source) is
//!   NOT built on ndarray/nalgebra/num-complex. Its `core`/`circuit`/`sim` crates
//!   have already migrated OFF all three onto `scirs2-core`, `scirs2-autograd`,
//!   `scirs2-linalg`, `scirs2-optimize`, `scirs2-sparse`, `scirs2-special`, and
//!   `optirs-core` — every removed dependency is left in place as a commented-out
//!   line tagged `# REMOVED: Use scirs2_core::* (SciRS2 POLICY)` in
//!   `open-source-libraries/quantrs/{core,circuit,sim}/Cargo.toml`. Vendoring
//!   QuantRS2 as-is would therefore import a **third** numeric stack (SciRS2)
//!   alongside `eg-numeric`'s ndarray+nalgebra — exactly the "numeric dualism" risk
//!   PROGRAM.md's quantum Risks section names and requires deciding "in Q1, not
//!   later." Recording it here, ahead of Q1, so Q1 starts from a decision rather
//!   than discovering the conflict mid-vendor.
//! - **Consequence for Q1:** vendoring QuantRS2 means stripping every `scirs2_*`
//!   numeric/linalg/complex/random call its `core`/`circuit`/`sim` crates make and
//!   re-targeting `eg-numeric` (extended with `Complex64` support) — the SAME kind of
//!   strip-and-align work Q1 is already chartered to do for Python-facing and
//!   research-only surface, just extended to the numeric layer too. `scirs2-graph`/
//!   `petgraph`/`pest` (circuit DSL parsing) and similar non-numeric QuantRS2
//!   dependencies are a separate question, not addressed by this decision.
//! - **Consequence for QuEST (Q3):** QuEST is used as an FFI backend behind
//!   [`backend::QuantumBackend`], not assimilated source — its internal C numeric
//!   representation stays inside the FFI boundary and never needs to unify with
//!   `eg-numeric`. Only the boundary types (arrays of `f64`/complex pairs crossing
//!   the FFI call) need a shape `eg-numeric` can hold, which a `Complex64` array
//!   extension already provides.

pub mod backend;
pub mod estimate;
pub mod hash;
pub mod ir;
pub mod planner;
pub mod qasm;
pub mod result;

pub use backend::{
    BackendCapabilities, BackendDescriptor, BackendError, BackendFamily, BackendId, JobHandle,
    JobStatus, QuantumBackend, RunOptions,
};
pub use estimate::{
    density_matrix_bytes, estimate, statevector_bytes, EntanglingConnectivity, Estimate,
    EstimateOptions, NoiseRequest,
};
pub use hash::{circuit_hash, CircuitHash, HashError};
pub use ir::{
    ClassicalBitRef, ClassicalRegister, ControlQubit, ControlState, GateInstruction, GateKind,
    Instruction, IrValidationError, ParamValue, Parameter, ProgramMetadata, QuantumProgram,
    IR_VERSION,
};
pub use planner::{
    select_backend, AuditEntry, PlannerDecision, PlannerError, PlannerOptions, PlannerRule,
};
pub use qasm::{from_qasm2, to_qasm2, QasmError};
pub use result::{Formalism, HardConstraint, NotExactError, Outcome, Proposal, QuantumResult};
