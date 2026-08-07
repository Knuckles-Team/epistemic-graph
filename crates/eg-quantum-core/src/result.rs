//! `QuantumResult` and the exactness type-state that keeps the epistemic layer sound.
//!
//! ★ The load-bearing rule (PROGRAM.md): only an EXACT result may ever become a hard
//! constraint; every other result — sampled, noisy, hardware-run, or anything from a
//! backend whose `is_exact_capable` capability is false — is a PROPOSAL that the
//! classical TMS/confidence/causal machinery must validate before any commit reaches
//! the normal `ChangeEnvelope`/`MutationBatch` path.
//!
//! A `bool` field on `QuantumResult` cannot enforce this by itself — any caller can
//! read `result.exact` and forget the check, or copy the value into a context that
//! never looks at it again. So exactness here is a TYPE, not just a flag:
//! `QuantumResult::into_hard_constraint` is the ONLY way to obtain a [`HardConstraint`],
//! it is fallible, and [`HardConstraint`] has no other public constructor anywhere in
//! this crate. A function whose signature takes `HardConstraint` (the epistemic
//! commit path, once it exists) statically cannot be handed an inexact result — there
//! is no `unsafe`, no "trust me" constructor, and no bypass short of literally
//! forging the private `Exactness::Exact` variant from inside this module.

use crate::backend::BackendId;
use crate::hash::CircuitHash;
use serde::{Deserialize, Serialize};

/// Which execution formalism actually produced this result. Distinct from
/// [`crate::backend::BackendFamily`] in spirit (this is a fact ABOUT a result;
/// `BackendFamily` is a fact about a backend) but intentionally the same vocabulary
/// shape so the two are trivially cross-referenced in observability (Q9).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Formalism {
    Statevector,
    DensityMatrix,
    Stabilizer,
    MatrixProductState,
    Trajectory,
    Hardware,
}

/// The backend-produced payload. Kept intentionally small and non-numeric at Q0 (no
/// simulator exists yet to produce amplitudes) — `Counts`/`ExpectationValue` are the
/// two shapes every downstream consumer (Q6 numeric bridge, Q7 epistemic hooks, Q8
/// agent-utilities API) actually needs, and both are already plain classical
/// data (`u64` counts, `f64` expectation) that belongs in `eg-numeric`'s RowSet/array
/// surfaces once Q6 wires it up — see the crate-level numeric-stack decision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Outcome {
    /// Measurement bitstring → shot count, e.g. `{"00": 480, "11": 520}`.
    Counts(std::collections::BTreeMap<String, u64>),
    /// A single scalar expectation value (e.g. `<psi|Z_0|psi>`), with an optional
    /// standard error (present for sampled/trajectory results, absent for exact ones).
    ExpectationValue { value: f64, stderr: Option<f64> },
}

/// Private exactness type-state. No `pub` anywhere on this type or its variants —
/// the ONLY way code outside this module can learn or act on exactness is through
/// `QuantumResult::is_exact()` (a read-only bool, fine for logging/observability) or
/// `QuantumResult::into_hard_constraint()` (the fallible, checked upgrade). Neither
/// path lets a caller construct a `HardConstraint` from an inexact result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Exactness {
    Exact,
    Inexact,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantumResult {
    pub backend_id: BackendId,
    pub formalism: Formalism,
    pub seed: Option<u64>,
    pub shots: Option<u64>,
    pub circuit_hash: CircuitHash,
    exactness: Exactness,
    pub noise_model_id: Option<String>,
    /// Backend-reported confidence in the result's fidelity, `[0.0, 1.0]` where
    /// applicable (e.g. hardware calibration-derived); `None` when the backend has
    /// no such estimate (most simulators: exact results have no fidelity concept,
    /// sampled results' quality is already captured by `Outcome`'s `stderr`).
    pub fidelity_hint: Option<f64>,
    pub wall_time_ms: u64,
    pub peak_memory_bytes: u64,
    pub outcome: Outcome,
}

/// Returned by [`QuantumResult::into_hard_constraint`] when the result is not exact.
/// Carries enough identity to log/audit the rejection without exposing the payload
/// (a rejected result is exactly the case where a caller must NOT quietly proceed as
/// if it had a constraint).
#[derive(Debug, Clone, thiserror::Error)]
#[error("QuantumResult from backend {backend_id} ({formalism:?}) is not exact and cannot become a hard constraint; treat it as a Proposal instead")]
pub struct NotExactError {
    pub backend_id: BackendId,
    pub formalism: Formalism,
}

impl QuantumResult {
    /// Construct an EXACT result. Callers are backends (or the planner's own
    /// bookkeeping); this is `pub` because a `QuantumBackend` implementation living in
    /// a downstream crate must be able to build one, but the exactness claim itself
    /// is auditable at the one call site — a backend that lies about exactness is a
    /// backend bug to fix there, not something this type can detect. R2/R1 planner
    /// acceptance tests (`tests/planner_acceptance.rs`) instead pin the STRUCTURAL
    /// rule ("a noise request never reaches a backend selection that could return
    /// this constructor") rather than trying to police it after the fact.
    #[allow(clippy::too_many_arguments)]
    pub fn new_exact(
        backend_id: BackendId,
        formalism: Formalism,
        seed: Option<u64>,
        shots: Option<u64>,
        circuit_hash: CircuitHash,
        wall_time_ms: u64,
        peak_memory_bytes: u64,
        outcome: Outcome,
    ) -> Self {
        QuantumResult {
            backend_id,
            formalism,
            seed,
            shots,
            circuit_hash,
            exactness: Exactness::Exact,
            noise_model_id: None,
            fidelity_hint: None,
            wall_time_ms,
            peak_memory_bytes,
            outcome,
        }
    }

    /// Construct an INEXACT result (sampled, noisy, or hardware-sourced).
    #[allow(clippy::too_many_arguments)]
    pub fn new_inexact(
        backend_id: BackendId,
        formalism: Formalism,
        seed: Option<u64>,
        shots: Option<u64>,
        circuit_hash: CircuitHash,
        noise_model_id: Option<String>,
        fidelity_hint: Option<f64>,
        wall_time_ms: u64,
        peak_memory_bytes: u64,
        outcome: Outcome,
    ) -> Self {
        QuantumResult {
            backend_id,
            formalism,
            seed,
            shots,
            circuit_hash,
            exactness: Exactness::Inexact,
            noise_model_id,
            fidelity_hint,
            wall_time_ms,
            peak_memory_bytes,
            outcome,
        }
    }

    /// Read-only. Safe to use for logging, telemetry, and UI display — NOT a
    /// substitute for `into_hard_constraint()` at any call site that will feed a
    /// commit path.
    pub fn is_exact(&self) -> bool {
        matches!(self.exactness, Exactness::Exact)
    }

    /// The ONLY path to a value the epistemic commit surface may treat as a hard
    /// constraint. Fails (returning the result's identity, not the payload) for
    /// anything not exact.
    pub fn into_hard_constraint(self) -> Result<HardConstraint, NotExactError> {
        match self.exactness {
            Exactness::Exact => Ok(HardConstraint(self)),
            Exactness::Inexact => Err(NotExactError {
                backend_id: self.backend_id,
                formalism: self.formalism,
            }),
        }
    }

    /// Always available. Every quantum result — exact or not — can be offered to the
    /// classical TMS as something to validate before it affects anything durable.
    pub fn into_proposal(self) -> Proposal {
        Proposal(self)
    }
}

/// A `QuantumResult` proven exact at construction time (via
/// `QuantumResult::into_hard_constraint`, the only constructor). This is the type the
/// epistemic commit path is meant to require in its signature once it exists (Q7) —
/// accepting `QuantumResult` there would let an inexact value slip through a
/// forgotten `if result.is_exact()`; accepting `HardConstraint` makes that class of
/// bug a compile error.
#[derive(Debug, Clone)]
pub struct HardConstraint(QuantumResult);

impl HardConstraint {
    pub fn result(&self) -> &QuantumResult {
        &self.0
    }

    pub fn into_result(self) -> QuantumResult {
        self.0
    }
}

/// A `QuantumResult` offered as a proposal — the ONLY thing quantum subroutines
/// produce that classical validation machinery consumes directly. Every
/// `QuantumResult` (including exact ones — an exact result is also a perfectly valid
/// proposal, just one the TMS can validate trivially).
#[derive(Debug, Clone)]
pub struct Proposal(QuantumResult);

impl Proposal {
    pub fn result(&self) -> &QuantumResult {
        &self.0
    }

    pub fn into_result(self) -> QuantumResult {
        self.0
    }
}
