//! `estimate()` — pure, backend-registry-independent circuit analysis.
//!
//! `estimate()` looks ONLY at the `QuantumProgram` and the caller's request shape
//! (`EstimateOptions`); it never sees which backends are actually registered. It
//! produces advisory `preferred`/`forbidden` [`BackendFamily`] rankings from
//! structural facts alone (Clifford-ness, memory footprint, noise request, and —
//! since register `D-QN-4` — a topology-only entanglement-connectivity classification;
//! see [`EntanglingConnectivity`]). The planner (`planner.rs`) is the step that
//! intersects this with what is actually available and applies rules R0-R5 to reach a
//! concrete [`BackendId`].

use crate::backend::{BackendFamily, BackendId};
use crate::ir::{Instruction, QuantumProgram};

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

/// A purely structural, **topology-only** proxy for how "entangled" a circuit's gate
/// pattern is — deliberately NOT a real Schmidt-rank or MPS bond-dimension
/// computation, because computing either honestly requires actually running/
/// simulating the circuit, which `estimate()` is designed not to need (there is no
/// simulator at this layer — see the module doc). It looks only at WHICH qubits each
/// instruction's multi-qubit gates connect; it never looks at gate angles, gate
/// repetition, or circuit depth.
///
/// # What it computes
///
/// Every [`Instruction::Gate`] that touches two or more qubits (targets + controls,
/// deduplicated) contributes one graph edge per pair of qubits it touches. From the
/// resulting graph over qubit indices:
///
/// - No edges at all → [`EntanglingConnectivity::Product`]: the circuit contains no
///   multi-qubit gate, so it is trivially bond-dimension-1 under any qubit ordering.
/// - Every edge connects qubits whose IR indices differ by exactly 1, AND no single
///   instruction touches 3+ qubits at once → [`EntanglingConnectivity::NearestNeighborChain`]:
///   the graph is necessarily a disjoint union of simple paths in qubit-index order —
///   exactly the topology 1-D tensor-network methods such as MPS are efficient for.
/// - Anything else (a long-range pair, a qubit connected to 3+ others, or any single
///   instruction entangling 3+ qubits at once) → [`EntanglingConnectivity::Dense`].
///
/// # Why this is a reasonable proxy, and exactly what it can get wrong
///
/// Nearest-neighbor-only entangling structure is the textbook case where a
/// fixed-order MPS needs only a small bond dimension to represent the state; a qubit
/// entangled with many far-apart partners is the textbook case where no fixed-order
/// MPS represents the state efficiently. That said, this proxy is honest about being
/// conservative in one direction and having a known blind spot in the other:
///
/// - **Conservative toward `Dense` (the safe direction for the planner's use of
///   this):** it does not special-case gates that provably cannot entangle a product
///   state on their own — e.g. `Swap`, a pure permutation, still counts as an
///   entangling edge, as does every `GateKind::Custom`. A single instruction touching
///   3+ qubits at once is always `Dense`, even if, e.g., it is a Toffoli acting
///   purely on basis states. So it can classify a circuit `Dense` when the real
///   entanglement is actually lower than that — a missed MPS-preference optimization
///   (falls back to statevector, still correct), never a false claim that a truly
///   dense circuit is low-entanglement.
/// - **Known blind spot toward `NearestNeighborChain` (the direction to watch):** it
///   is topology-only and ignores circuit depth / gate repetition entirely. A
///   strictly nearest-neighbor circuit run to very large depth CAN still require a
///   large bond dimension in practice — entanglement entropy across a cut in a 1-D
///   chain grows with depth before saturating — and this classification has no way
///   to see that; it will still report `NearestNeighborChain` for such a circuit.
///   Detecting that honestly requires either simulating or a real entanglement-
///   entropy estimate, which is out of scope for a `QuantumProgram`-only structural
///   pass. `NearestNeighborChain` should therefore be read as "the topology alone
///   does not force high bond dimension," not as a bond-dimension guarantee — which
///   is exactly the strength `planner.rs`'s R3 rule relies on it for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntanglingConnectivity {
    /// No multi-qubit gate anywhere in the program.
    Product,
    /// Every multi-qubit gate connects qubits adjacent in IR index order, and no
    /// single instruction touches 3+ qubits at once.
    NearestNeighborChain,
    /// A long-range pair, a qubit connected to 3+ others, or an instruction touching
    /// 3+ qubits at once — structurally consistent with bond dimension exploding
    /// along any fixed linear qubit ordering.
    Dense,
}

/// Compute [`EntanglingConnectivity`] for `program`. See that type's docs for exactly
/// what this does and does not prove.
fn classify_entangling_connectivity(program: &QuantumProgram) -> EntanglingConnectivity {
    use std::collections::{BTreeMap, BTreeSet};

    let mut edges: BTreeSet<(u32, u32)> = BTreeSet::new();
    let mut any_wide_gate = false; // a single instruction touching 3+ qubits at once

    for instr in &program.instructions {
        if let Instruction::Gate(g) = instr {
            let mut touched = g.qubits.clone();
            touched.extend(g.controls.iter().map(|c| c.qubit));
            touched.sort_unstable();
            touched.dedup();
            if touched.len() < 2 {
                continue; // a single-qubit gate cannot entangle anything by itself
            }
            if touched.len() > 2 {
                any_wide_gate = true;
            }
            for (i, &a) in touched.iter().enumerate() {
                for &b in &touched[i + 1..] {
                    edges.insert((a.min(b), a.max(b)));
                }
            }
        }
    }

    if edges.is_empty() {
        return EntanglingConnectivity::Product;
    }
    if any_wide_gate {
        return EntanglingConnectivity::Dense;
    }

    let mut degree: BTreeMap<u32, u32> = BTreeMap::new();
    for &(a, b) in &edges {
        if b - a != 1 {
            return EntanglingConnectivity::Dense; // long-range pair
        }
        *degree.entry(a).or_insert(0) += 1;
        *degree.entry(b).or_insert(0) += 1;
    }
    // With only nearest-neighbor edges (checked above) and deduplicated edges,
    // degree cannot exceed 2 (a qubit index has at most two integer neighbors) —
    // kept as an explicit, defensive check rather than an invariant assumed
    // silently, in case the edge-generation logic above ever changes.
    if degree.values().any(|&d| d > 2) {
        return EntanglingConnectivity::Dense;
    }

    EntanglingConnectivity::NearestNeighborChain
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
    /// Topology-only entanglement-connectivity classification driving planner rule
    /// R3 — see [`EntanglingConnectivity`] for exactly what this does and does not
    /// prove (NOT a real bond-dimension computation).
    pub entangling_connectivity: EntanglingConnectivity,
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
    let entangling_connectivity = classify_entangling_connectivity(program);

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
        // GPU-preferred over CPU when it fits. R3: MatrixProductState is ranked
        // AHEAD of statevector when `entangling_connectivity` genuinely supports it
        // (Product/NearestNeighborChain), and is not offered as a preferred candidate
        // at all when it does not (Dense) — "reject if bond dim explodes" per
        // PROGRAM.md's R3. See `EntanglingConnectivity` docs for what this
        // classification does and does not prove.
        match entangling_connectivity {
            EntanglingConnectivity::Product | EntanglingConnectivity::NearestNeighborChain => {
                preferred.push(BackendFamily::MatrixProductState);
                preferred.push(BackendFamily::StatevectorGpu);
                preferred.push(BackendFamily::StatevectorCpu);
            }
            EntanglingConnectivity::Dense => {
                preferred.push(BackendFamily::StatevectorGpu);
                preferred.push(BackendFamily::StatevectorCpu);
            }
        }
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
        entangling_connectivity,
        preferred,
        forbidden,
        est_time_ms: estimate_time_ms(n_qubits, depth),
    }
}
