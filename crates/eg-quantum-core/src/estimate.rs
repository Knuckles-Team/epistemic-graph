//! `estimate()` — pure, backend-registry-independent circuit analysis.
//!
//! `estimate()` looks ONLY at the `QuantumProgram` and the caller's request shape
//! (`EstimateOptions`); it never sees which backends are actually registered. It
//! produces advisory `preferred`/`forbidden` [`BackendFamily`] rankings from
//! structural facts alone (Clifford-ness, memory footprint, noise request). The
//! planner (`planner.rs`) is the step that intersects this with what is actually
//! available and applies rules R0-R5 to reach a concrete [`BackendId`].

use crate::backend::{BackendFamily, BackendId};
use crate::ir::QuantumProgram;

/// A caller-declared property of a requested noise model: whether it is restricted
/// to Clifford-preserving channels (e.g. depolarizing/Pauli-twirled noise at fixed
/// rates) or includes anything else. `estimate()` cannot infer this from the circuit
/// (the circuit says nothing about the noise channel), so it is conservatively
/// required as input rather than guessed.
#[derive(Debug, Clone, PartialEq)]
pub struct NoiseRequest {
    pub model_id: Option<String>,
    /// Conservative default when constructed via `NoiseRequest::declared(model_id)`
    /// is `true` (non-Clifford) — an unrecognized/undeclared noise model must never
    /// be silently treated as Clifford-safe, since that would let a non-Clifford-noise
    /// request slip past planner rule R1's `!has_non_clifford_noise` guard.
    pub non_clifford: bool,
}

impl NoiseRequest {
    /// A noise model declared WITHOUT an explicit Clifford-safety claim — defaults to
    /// `non_clifford: true` (the safe assumption).
    pub fn declared(model_id: impl Into<String>) -> Self {
        NoiseRequest {
            model_id: Some(model_id.into()),
            non_clifford: true,
        }
    }

    /// A noise model explicitly asserted Clifford-preserving by the caller.
    pub fn clifford_preserving(model_id: impl Into<String>) -> Self {
        NoiseRequest {
            model_id: Some(model_id.into()),
            non_clifford: false,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct EstimateOptions {
    pub noise: Option<NoiseRequest>,
    /// Caller wants the exact density matrix (not shot samples) if noise is present.
    pub want_exact_density_matrix: bool,
    pub shots: Option<u64>,
    /// A hard ceiling in bytes; `estimate()` uses this to populate `forbidden` (any
    /// family whose footprint at this circuit size would exceed it) and the planner
    /// (R0) treats it as non-negotiable.
    pub memory_bound_bytes: Option<u64>,
    /// The R5 escape hatch: an explicit backend selection that, if honoured, bypasses
    /// R0-R4 entirely. `estimate()` does not act on this itself — it is threaded
    /// through to `planner::select_backend`'s `PlannerOptions` unchanged. Present here
    /// too only so `Estimate` and the eventual audit record can be built from ONE
    /// options value if a caller prefers that.
    pub backend_id_override: Option<BackendId>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Estimate {
    pub n_qubits: u32,
    pub depth: u32,
    pub is_clifford: bool,
    pub has_non_clifford_noise: bool,
    pub requires_density_matrix: bool,
    /// Bytes for a full complex128 statevector: `16 * 2^n_qubits`, saturating at
    /// `u64::MAX` rather than overflowing/panicking for large `n_qubits`.
    pub mem_bytes_sv: u64,
    /// Bytes for a full complex128 density matrix: `16 * 4^n_qubits`, saturating.
    pub mem_bytes_dm: u64,
    pub preferred: Vec<BackendFamily>,
    pub forbidden: Vec<BackendFamily>,
    /// A rough, documented-placeholder cost heuristic (see `estimate_time_ms`) —
    /// NOT a scheduling promise. Real per-backend timing lands in Q3/Q12 once actual
    /// backends exist to benchmark.
    pub est_time_ms: u64,
}

/// `16 * 2^n`, saturating.
pub fn statevector_bytes(n_qubits: u32) -> u64 {
    16u64.saturating_mul(1u64.checked_shl(n_qubits.min(63)).unwrap_or(u64::MAX))
}

/// `16 * 4^n` == `16 * (2^n)^2`, saturating.
pub fn density_matrix_bytes(n_qubits: u32) -> u64 {
    let sv = statevector_bytes(n_qubits) / 16; // 2^n, saturating via statevector_bytes
    sv.saturating_mul(sv).saturating_mul(16)
}

/// Placeholder cost heuristic: exponential in qubit count (capped), linear in depth,
/// deliberately crude. Documented as a placeholder rather than left unexplained so a
/// future lane does not mistake it for a calibrated model.
fn estimate_time_ms(n_qubits: u32, depth: u32) -> u64 {
    let capped_n = n_qubits.min(40); // avoid absurd shift; anything past this is "very large" either way
    let state_space = 1u64.checked_shl(capped_n).unwrap_or(u64::MAX);
    state_space.saturating_mul(depth.max(1) as u64) / 1_000_000
}

pub fn estimate(program: &QuantumProgram, opts: &EstimateOptions) -> Estimate {
    let n_qubits = program.n_qubits;
    let depth = program.depth();
    let is_clifford = program.is_clifford();
    let has_non_clifford_noise = opts.noise.as_ref().map(|n| n.non_clifford).unwrap_or(false);
    let requires_density_matrix =
        opts.want_exact_density_matrix || (opts.noise.is_some() && opts.shots.is_none());

    let mem_bytes_sv = statevector_bytes(n_qubits);
    let mem_bytes_dm = density_matrix_bytes(n_qubits);
    let bound = opts.memory_bound_bytes.unwrap_or(u64::MAX);

    let mut preferred = Vec::new();
    let mut forbidden = Vec::new();

    // R1 affinity: a Clifford circuit with no non-Clifford noise request is always
    // O(n^2)-representable exactly by the stabilizer formalism — surface that first
    // regardless of what else is requested, since paying O(2^n) for it is never
    // justified.
    if is_clifford && !has_non_clifford_noise {
        preferred.push(BackendFamily::Stabilizer);
    }

    if let Some(noise) = &opts.noise {
        if requires_density_matrix {
            preferred.push(BackendFamily::DensityMatrixCpu);
            preferred.push(BackendFamily::DensityMatrixGpu);
            preferred.push(BackendFamily::QuestFfi);
        } else {
            preferred.push(BackendFamily::Trajectory);
        }
        let _ = noise; // model_id currently informational only at Q0 (no noise-model catalog yet)
    } else if !is_clifford || has_non_clifford_noise {
        // No noise requested and not the Clifford fast-path: statevector family,
        // GPU-preferred over CPU when it fits, plus MPS as a structural alternative
        // the planner may pick under R3 once it has genuine entanglement data (Q4).
        preferred.push(BackendFamily::StatevectorGpu);
        preferred.push(BackendFamily::StatevectorCpu);
        preferred.push(BackendFamily::MatrixProductState);
        preferred.push(BackendFamily::QuestFfi);
    }

    // R0 hard elimination: a family whose EXACT memory footprint at this circuit size
    // would exceed the caller's bound is forbidden outright — never a candidate the
    // planner can silently fall back to.
    if mem_bytes_sv > bound {
        forbidden.push(BackendFamily::StatevectorCpu);
        forbidden.push(BackendFamily::StatevectorGpu);
    }
    if requires_density_matrix && mem_bytes_dm > bound {
        forbidden.push(BackendFamily::DensityMatrixCpu);
        forbidden.push(BackendFamily::DensityMatrixGpu);
    }

    Estimate {
        n_qubits,
        depth,
        is_clifford,
        has_non_clifford_noise,
        requires_density_matrix,
        mem_bytes_sv,
        mem_bytes_dm,
        preferred,
        forbidden,
        est_time_ms: estimate_time_ms(n_qubits, depth),
    }
}
