//! [`StabilizerSimulator`] -- an Aaronson-Gottesman CHP tableau backend
//! (Aaronson & Gottesman, "Improved Simulation of Stabilizer Circuits", 2004).
//! O(n^2) space and O(n^2) time per gate/measurement, versus O(2^n) for
//! [`crate::statevector::StateVectorSimulator`] -- the exponential-vs-polynomial
//! gap that makes planner rule R1 (route a Clifford circuit here, never to
//! statevector) the highest-value routing decision in the whole quantum program.
//!
//! This is a clean-room implementation of the published algorithm against
//! `eg-quantum-core`'s IR, not a port of QuantRS2's `sim/src/stabilizer/{types,
//! functions}.rs` -- those files are coupled to `scirs2_core::{ndarray, random}`
//! (see `crate` module doc for the fuller numeric-stack rationale) and the
//! Aaronson-Gottesman tableau is public, well-specified algorithm content, not
//! QuantRS2-original expression to vendor.
//!
//! # Tableau representation
//!
//! `2n` rows over `n` qubits: rows `0..n` are the destabilizer generators, rows
//! `n..2n` are the stabilizer generators. Each row is a Pauli string, stored as an
//! `n`-bit `x` vector, an `n`-bit `z` vector (qubit `j`'s Pauli is `I` if
//! `x[j]=z[j]=0`, `X` if `x[j]=1,z[j]=0`, `Z` if `x[j]=0,z[j]=1`, `Y` if
//! `x[j]=z[j]=1`), and a phase bit `r` (`true` = the generator's overall sign is
//! `-1`). The initial state `|0...0>` has destabilizer `i` = `X_i` and stabilizer
//! `i` = `Z_i`.

use eg_quantum_core::backend::{
    BackendCapabilities, BackendError, BackendFamily, BackendId, JobHandle, JobStatus,
    QuantumBackend, RunOptions,
};
use eg_quantum_core::ir::{ControlQubit, ControlState, GateKind, Instruction, QuantumProgram};
use eg_quantum_core::result::{Formalism, Outcome, QuantumResult};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::{resolve_params, ClassicalMemory, SimError};

/// The CHP tableau. `pub` (appears in `evolve`'s public signature) but every field
/// is private -- callers observe it only through [`Tableau::measure`] and the
/// backend's `Outcome`, never the raw bits, which is deliberately NOT a stable
/// public wire format at Q1.
#[derive(Debug, Clone)]
pub struct Tableau {
    n: usize,
    x: Vec<Vec<bool>>,
    z: Vec<Vec<bool>>,
    r: Vec<bool>,
}

impl Tableau {
    /// The `|0...0>` state: destabilizer `i` = `X_i`, stabilizer `i` = `Z_i`.
    pub fn zero_state(n: usize) -> Self {
        let mut x = vec![vec![false; n]; 2 * n];
        let mut z = vec![vec![false; n]; 2 * n];
        for i in 0..n {
            x[i][i] = true; // destabilizer i = X_i
            z[n + i][i] = true; // stabilizer i = Z_i
        }
        Tableau {
            n,
            x,
            z,
            r: vec![false; 2 * n],
        }
    }

    fn h(&mut self, a: u32) {
        let a = a as usize;
        for i in 0..2 * self.n {
            self.r[i] ^= self.x[i][a] && self.z[i][a];
            std::mem::swap(&mut self.x[i][a], &mut self.z[i][a]);
        }
    }

    fn s(&mut self, a: u32) {
        let a = a as usize;
        for i in 0..2 * self.n {
            self.r[i] ^= self.x[i][a] && self.z[i][a];
            self.z[i][a] ^= self.x[i][a];
        }
    }

    fn sdg(&mut self, a: u32) {
        // S^-1 == S^3 (S^4 == I); three applications is simple and unambiguously
        // correct against the single `s` primitive above, no separate formula to
        // get wrong.
        self.s(a);
        self.s(a);
        self.s(a);
    }

    /// Pauli-X conjugation: flips the phase of every row whose Pauli at qubit `a`
    /// anticommutes with X (i.e. has a Z component: Z or Y).
    fn x_gate(&mut self, a: u32) {
        let a = a as usize;
        for i in 0..2 * self.n {
            self.r[i] ^= self.z[i][a];
        }
    }

    /// Pauli-Z conjugation: flips the phase of every row whose Pauli at qubit `a`
    /// anticommutes with Z (i.e. has an X component: X or Y).
    fn z_gate(&mut self, a: u32) {
        let a = a as usize;
        for i in 0..2 * self.n {
            self.r[i] ^= self.x[i][a];
        }
    }

    /// Pauli-Y conjugation: flips the phase of every row whose Pauli at qubit `a`
    /// anticommutes with Y (i.e. is X or Z, but not I or Y itself) -- exactly the
    /// rows where `x XOR z` is true.
    fn y_gate(&mut self, a: u32) {
        let a = a as usize;
        for i in 0..2 * self.n {
            self.r[i] ^= self.x[i][a] ^ self.z[i][a];
        }
    }

    fn swap(&mut self, a: u32, b: u32) {
        let (a, b) = (a as usize, b as usize);
        for i in 0..2 * self.n {
            self.x[i].swap(a, b);
            self.z[i].swap(a, b);
        }
    }

    /// CNOT: control `a`, target `b`. The standard Aaronson-Gottesman update rule.
    fn cnot(&mut self, a: u32, b: u32) {
        let (a, b) = (a as usize, b as usize);
        for i in 0..2 * self.n {
            let (xa, za, xb, zb) = (self.x[i][a], self.z[i][a], self.x[i][b], self.z[i][b]);
            self.r[i] ^= xa && zb && (xb ^ za ^ true);
            self.x[i][b] ^= xa;
            self.z[i][a] ^= zb;
        }
    }

    /// CZ(a,b) = H(b) . CNOT(a,b) . H(b) -- standard decomposition, symmetric in a/b.
    fn cz(&mut self, a: u32, b: u32) {
        self.h(b);
        self.cnot(a, b);
        self.h(b);
    }

    /// CY(a,b) = S(b) . CNOT(a,b) . S†(b), since `S X S† = Y` exactly (verified by
    /// direct 2x2 matrix multiplication: S=diag(1,i), S X S† = [[0,-i],[i,0]] = Y).
    fn cy(&mut self, a: u32, b: u32) {
        self.sdg(b);
        self.cnot(a, b);
        self.s(b);
    }

    /// The `g` phase-exponent function from Aaronson-Gottesman Section III: for two
    /// single-qubit Paulis `P1=(x1,z1)`, `P2=(x2,z2)`, `P1 . P2 = i^g(x1,z1,x2,z2) .
    /// P(x1^x2, z1^z2)`. All four branches independently verified by hand against
    /// the six nontrivial Pauli products (XY=iZ, YX=-iZ, YZ=iX, ZY=-iX, ZX=iY,
    /// XZ=-iY) in this lane's own derivation notes.
    fn g(x1: bool, z1: bool, x2: bool, z2: bool) -> i32 {
        let (x2, z2) = (x2 as i32, z2 as i32);
        if !x1 && !z1 {
            0
        } else if x1 && z1 {
            z2 - x2
        } else if x1 && !z1 {
            z2 * (2 * x2 - 1)
        } else {
            // !x1 && z1
            x2 * (1 - 2 * z2)
        }
    }

    /// `rowsum(h, i)`: row `h` becomes `row(h) * row(i)` (Pauli-string
    /// multiplication, phases combined via [`Tableau::g`]).
    fn rowsum(&mut self, h: usize, i: usize) {
        let mut sum: i32 = 2 * (self.r[h] as i32) + 2 * (self.r[i] as i32);
        for j in 0..self.n {
            sum += Self::g(self.x[i][j], self.z[i][j], self.x[h][j], self.z[h][j]);
        }
        let sum_mod4 = sum.rem_euclid(4);
        debug_assert!(
            sum_mod4 == 0 || sum_mod4 == 2,
            "rowsum phase sum {sum} (mod4={sum_mod4}) is neither 0 nor 2 -- tableau invariant violated"
        );
        self.r[h] = sum_mod4 == 2;
        for j in 0..self.n {
            self.x[h][j] ^= self.x[i][j];
            self.z[h][j] ^= self.z[i][j];
        }
    }

    /// Measure qubit `a` in the computational (Z) basis, collapsing the tableau in
    /// place, using `sample01()` to resolve a genuinely random outcome. Returns the
    /// classical bit.
    pub fn measure(&mut self, a: u32, sample01: impl FnOnce() -> bool) -> bool {
        let a = a as usize;
        // Is there a stabilizer row (n..2n) that anticommutes with Z_a (x[p][a] ==
        // true)? If so the outcome is genuinely random; take the FIRST such p, per
        // the published algorithm.
        let p = (self.n..2 * self.n).find(|&i| self.x[i][a]);
        match p {
            Some(p) => {
                for i in 0..2 * self.n {
                    if i != p && self.x[i][a] {
                        self.rowsum_pair(i, p);
                    }
                }
                // Move the about-to-be-overwritten stabilizer down into its
                // destabilizer slot before overwriting row p.
                let dest = p - self.n;
                self.x[dest] = self.x[p].clone();
                self.z[dest] = self.z[p].clone();
                self.r[dest] = self.r[p];
                // Row p becomes the new stabilizer Z_a with a fresh random sign.
                for j in 0..self.n {
                    self.x[p][j] = false;
                    self.z[p][j] = j == a;
                }
                let outcome = sample01();
                self.r[p] = outcome;
                outcome
            }
            None => {
                // Deterministic outcome: accumulate into a scratch row (index n,
                // reusing a temporary outside the real 2n rows) the product of every
                // stabilizer row whose CORRESPONDING destabilizer anticommutes with
                // Z_a.
                let mut scratch_x = vec![false; self.n];
                let mut scratch_z = vec![false; self.n];
                let mut scratch_r = false;
                for i in 0..self.n {
                    if self.x[i][a] {
                        Self::rowsum_into_scratch(
                            &mut scratch_x,
                            &mut scratch_z,
                            &mut scratch_r,
                            &self.x[self.n + i],
                            &self.z[self.n + i],
                            self.r[self.n + i],
                            self.n,
                        );
                    }
                }
                scratch_r
            }
        }
    }

    /// `rowsum(h, i)` where both `h` and `i` are real row indices (used by the
    /// random-outcome branch of `measure`).
    fn rowsum_pair(&mut self, h: usize, i: usize) {
        self.rowsum(h, i);
    }

    /// The same phase/bit combination `rowsum` performs, applied to an external
    /// scratch accumulator instead of a real tableau row (used by the deterministic
    /// branch of `measure`, which per the algorithm never mutates the tableau
    /// itself).
    #[allow(clippy::too_many_arguments)]
    fn rowsum_into_scratch(
        scratch_x: &mut [bool],
        scratch_z: &mut [bool],
        scratch_r: &mut bool,
        row_x: &[bool],
        row_z: &[bool],
        row_r: bool,
        n: usize,
    ) {
        let mut sum: i32 = 2 * (*scratch_r as i32) + 2 * (row_r as i32);
        for j in 0..n {
            sum += Self::g(row_x[j], row_z[j], scratch_x[j], scratch_z[j]);
        }
        let sum_mod4 = sum.rem_euclid(4);
        *scratch_r = sum_mod4 == 2;
        for j in 0..n {
            scratch_x[j] ^= row_x[j];
            scratch_z[j] ^= row_z[j];
        }
    }
}

/// Apply one Clifford [`GateInstruction`] to the tableau. Rejects (returns
/// [`SimError::NotClifford`]) anything `GateInstruction::is_clifford()` would call
/// non-Clifford -- this function is the enforcement point for "a stabilizer backend
/// can only ever represent a Clifford circuit," mirrored by the crate-level
/// `program.is_clifford()` check `evolve` performs up front.
fn apply_clifford_gate(
    tab: &mut Tableau,
    g: &eg_quantum_core::ir::GateInstruction,
) -> Result<(), SimError> {
    if !g.is_clifford() {
        return Err(SimError::NotClifford(Box::new(g.clone())));
    }
    match (g.controls.as_slice(), &g.gate) {
        ([], GateKind::Id) => {}
        ([], GateKind::X) => tab.x_gate(g.qubits[0]),
        ([], GateKind::Y) => tab.y_gate(g.qubits[0]),
        ([], GateKind::Z) => tab.z_gate(g.qubits[0]),
        ([], GateKind::H) => tab.h(g.qubits[0]),
        ([], GateKind::S) => tab.s(g.qubits[0]),
        ([], GateKind::Sdg) => tab.sdg(g.qubits[0]),
        ([], GateKind::Swap) => tab.swap(g.qubits[0], g.qubits[1]),
        ([ControlQubit { qubit: ctrl, state }], GateKind::X) => {
            apply_single_controlled(tab, *ctrl, *state, g.qubits[0], Tableau::cnot)
        }
        ([ControlQubit { qubit: ctrl, state }], GateKind::Y) => {
            apply_single_controlled(tab, *ctrl, *state, g.qubits[0], Tableau::cy)
        }
        ([ControlQubit { qubit: ctrl, state }], GateKind::Z) => {
            apply_single_controlled(tab, *ctrl, *state, g.qubits[0], Tableau::cz)
        }
        // is_clifford() already rejected every other shape; unreachable in practice,
        // but fail loudly rather than silently no-op if the IR's Clifford
        // vocabulary ever grows without this match being updated.
        _ => return Err(SimError::NotClifford(Box::new(g.clone()))),
    }
    Ok(())
}

/// A negative control (`ControlState::Zero`) is X-conjugated on the control qubit
/// before and after the positive-control primitive -- the standard trick (fire on
/// `|0>` == fire an X-sandwiched version of the `|1>`-firing gate).
fn apply_single_controlled(
    tab: &mut Tableau,
    ctrl: u32,
    state: ControlState,
    target: u32,
    op: fn(&mut Tableau, u32, u32),
) {
    let negate = matches!(state, ControlState::Zero);
    if negate {
        tab.x_gate(ctrl);
    }
    op(&mut *tab, ctrl, target);
    if negate {
        tab.x_gate(ctrl);
    }
}

/// One coherent evolution of a Clifford [`QuantumProgram`] through the tableau,
/// recording classical-bit outcomes for every `Measure` instruction. Mirrors
/// `statevector::evolve`'s shape so both backends are drop-in equivalent for a
/// caller that only wants classical outcomes.
pub fn evolve(
    program: &QuantumProgram,
    rng: &mut eg_numeric::random::Generator,
) -> Result<(Tableau, ClassicalMemory), SimError> {
    let mut tab = Tableau::zero_state(program.n_qubits as usize);
    let mut classical = ClassicalMemory::default();
    // No continuous parameters exist on a Clifford instruction (see
    // `GateInstruction::is_clifford`'s own doc: any nonzero `params` is
    // non-Clifford), so an empty binding map is correct here; the stabilizer path
    // never needs `RunOptions.parameter_bindings`.
    let empty_bindings = BTreeMap::new();
    for instr in &program.instructions {
        match instr {
            Instruction::Gate(g) => {
                // resolve_params is called only to surface a consistent
                // UnboundParameter/arity error shape if a future IR change ever
                // lets a Clifford gate carry a param; today `is_clifford()` already
                // guarantees `g.params.is_empty()`.
                let _ = resolve_params(g, &empty_bindings)?;
                apply_clifford_gate(&mut tab, g)?;
            }
            Instruction::Measure {
                qubit,
                classical_bit,
            } => {
                let outcome = tab.measure(*qubit, || rng.uniform(0.0, 1.0, 1)[0] < 0.5);
                classical.set(&classical_bit.register, classical_bit.index, outcome);
            }
            Instruction::Reset { qubit } => {
                let outcome = tab.measure(*qubit, || rng.uniform(0.0, 1.0, 1)[0] < 0.5);
                if outcome {
                    tab.x_gate(*qubit);
                }
            }
            Instruction::Barrier { .. } => {}
        }
    }
    Ok((tab, classical))
}

struct JobStore {
    next: AtomicU64,
    completed: Mutex<std::collections::HashMap<u64, QuantumResult>>,
}

impl Default for JobStore {
    fn default() -> Self {
        JobStore {
            next: AtomicU64::new(0),
            completed: Mutex::new(std::collections::HashMap::new()),
        }
    }
}

/// An Aaronson-Gottesman tableau [`QuantumBackend`]. Same in-process/synchronous
/// job-store shape as [`crate::statevector::StateVectorSimulator`]; the only
/// difference in `execute` is that it rejects a non-Clifford circuit up front
/// (`program.is_clifford()`) rather than the qubit-count ceiling statevector uses --
/// a stabilizer circuit's O(n^2) footprint makes qubit count nearly irrelevant at
/// smoke-test scale, but REPRESENTABILITY (Clifford-only) is the hard constraint.
pub struct StabilizerSimulator {
    id: BackendId,
    max_qubits: u32,
    jobs: JobStore,
}

impl Default for StabilizerSimulator {
    fn default() -> Self {
        Self::new()
    }
}

impl StabilizerSimulator {
    pub fn new() -> Self {
        StabilizerSimulator {
            id: BackendId::from("stabilizer"),
            // O(n^2), not O(2^n) -- generous compared to the statevector cap; still
            // a finite placeholder ceiling for Q1, not a claim about the tableau
            // algorithm's real limit.
            max_qubits: 512,
            jobs: JobStore::default(),
        }
    }

    fn execute(
        &self,
        program: &QuantumProgram,
        opts: &RunOptions,
    ) -> Result<QuantumResult, BackendError> {
        program
            .validate()
            .map_err(|e| BackendError::InvalidProgram(e.to_string()))?;
        if opts.noise_model_id.is_some() {
            return Err(BackendError::Unsupported(self.id.clone()));
        }
        if !program.is_clifford() {
            return Err(BackendError::InvalidProgram(
                "circuit is not Clifford; the stabilizer backend cannot represent it".to_string(),
            ));
        }
        if program.n_qubits > self.max_qubits {
            return Err(BackendError::ResourceLimit(format!(
                "n_qubits={} exceeds this backend's placeholder max_qubits={}",
                program.n_qubits, self.max_qubits
            )));
        }
        let circuit_hash = program
            .circuit_hash()
            .map_err(|e| BackendError::InvalidProgram(e.to_string()))?;
        let shots = opts.shots.unwrap_or(1);
        let start = std::time::Instant::now();
        let mut counts: BTreeMap<String, u64> = BTreeMap::new();
        for shot in 0..shots {
            let seed = opts.seed.unwrap_or(0).wrapping_add(shot);
            let mut rng = eg_numeric::random::Generator::new(seed);
            let (_tab, classical) =
                evolve(program, &mut rng).map_err(|e| BackendError::Execution(e.to_string()))?;
            let key = classical.bitstring(&program.classical_registers);
            *counts.entry(key).or_insert(0) += 1;
        }
        let wall_time_ms = start.elapsed().as_millis() as u64;
        // Tableau footprint: 2n rows * n columns * 2 bits (x,z), plus n phase bits --
        // approximated in bytes (bool storage, not bit-packed, so 1 byte/entry here).
        let n = program.n_qubits as u64;
        let peak_memory_bytes = 2 * n * n * 2 + 2 * n;
        Ok(QuantumResult::new_exact(
            self.id.clone(),
            Formalism::Stabilizer,
            opts.seed,
            Some(shots),
            circuit_hash,
            wall_time_ms,
            peak_memory_bytes,
            Outcome::Counts(counts),
        ))
    }
}

impl QuantumBackend for StabilizerSimulator {
    fn backend_id(&self) -> BackendId {
        self.id.clone()
    }

    fn family(&self) -> BackendFamily {
        BackendFamily::Stabilizer
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            supports_density_matrix: false,
            supports_distributed: false,
            supports_noise: false,
            supports_gpu: false,
            supports_mps: false,
            supports_stabilizer: true,
            is_exact_capable: true,
            max_qubits_statevector: None,
            max_qubits_density_matrix: None,
            requires_hardware: false,
        }
    }

    fn submit(
        &self,
        program: &QuantumProgram,
        opts: &RunOptions,
    ) -> Result<JobHandle, BackendError> {
        let result = self.execute(program, opts)?;
        let handle = self.jobs.next.fetch_add(1, Ordering::SeqCst);
        self.jobs
            .completed
            .lock()
            .expect("job store mutex poisoned")
            .insert(handle, result);
        Ok(JobHandle(handle))
    }

    fn poll(&self, job: JobHandle) -> Result<JobStatus, BackendError> {
        if self
            .jobs
            .completed
            .lock()
            .expect("job store mutex poisoned")
            .contains_key(&job.0)
        {
            Ok(JobStatus::Completed)
        } else {
            Err(BackendError::UnknownJob)
        }
    }

    fn result(&self, job: JobHandle) -> Result<QuantumResult, BackendError> {
        self.jobs
            .completed
            .lock()
            .expect("job store mutex poisoned")
            .get(&job.0)
            .cloned()
            .ok_or(BackendError::UnknownJob)
    }

    fn cancel(&self, job: JobHandle) -> Result<(), BackendError> {
        if self
            .jobs
            .completed
            .lock()
            .expect("job store mutex poisoned")
            .contains_key(&job.0)
        {
            Ok(())
        } else {
            Err(BackendError::UnknownJob)
        }
    }

    fn run(
        &self,
        program: &QuantumProgram,
        opts: &RunOptions,
    ) -> Result<QuantumResult, BackendError> {
        self.execute(program, opts)
    }
}

#[cfg(test)]
mod tableau_tests {
    use super::*;

    #[test]
    fn bell_pair_measurement_is_perfectly_correlated() {
        // H(0); CNOT(0,1); measure both. Across many seeds, qubit0's outcome must
        // equal qubit1's outcome EVERY time (the deterministic half of the
        // algorithm), and both 0 and 1 must occur (the random half).
        let mut saw_00 = false;
        let mut saw_11 = false;
        for seed in 0..64u64 {
            let mut tab = Tableau::zero_state(2);
            tab.h(0);
            tab.cnot(0, 1);
            let mut rng = eg_numeric::random::Generator::new(seed);
            let o0 = tab.measure(0, || rng.uniform(0.0, 1.0, 1)[0] < 0.5);
            let o1 = tab.measure(1, || rng.uniform(0.0, 1.0, 1)[0] < 0.5);
            assert_eq!(o0, o1, "Bell pair outcomes diverged at seed {seed}");
            saw_00 |= !o0;
            saw_11 |= o0;
        }
        assert!(saw_00 && saw_11, "expected both outcomes across 64 seeds");
    }

    #[test]
    fn ghz_three_qubit_all_outcomes_equal() {
        for seed in 0..32u64 {
            let mut tab = Tableau::zero_state(3);
            tab.h(0);
            tab.cnot(0, 1);
            tab.cnot(0, 2);
            let mut rng = eg_numeric::random::Generator::new(seed);
            let o0 = tab.measure(0, || rng.uniform(0.0, 1.0, 1)[0] < 0.5);
            let o1 = tab.measure(1, || rng.uniform(0.0, 1.0, 1)[0] < 0.5);
            let o2 = tab.measure(2, || rng.uniform(0.0, 1.0, 1)[0] < 0.5);
            assert_eq!(o0, o1);
            assert_eq!(o1, o2);
        }
    }

    #[test]
    fn cz_and_cy_decompositions_are_involutions_up_to_bookkeeping() {
        // Applying the same controlled gate twice returns the tableau to the
        // initial |0> stabilizer generators (every one of these gates is a Clifford
        // involution up to global phase tracked in `r`, and starting from |0..0>
        // the whole state is a +1 eigenstate for all of them, so two applications
        // must return exactly to X_i/Z_i generators with r all false).
        let base = Tableau::zero_state(2);
        let mut tab = base.clone();
        tab.cz(0, 1);
        tab.cz(0, 1);
        assert_eq!(tab.x, base.x);
        assert_eq!(tab.z, base.z);
        assert_eq!(tab.r, base.r);

        let mut tab2 = base.clone();
        tab2.cy(0, 1);
        tab2.cy(0, 1);
        assert_eq!(tab2.x, base.x);
        assert_eq!(tab2.z, base.z);
        assert_eq!(tab2.r, base.r);
    }

    #[test]
    fn swap_then_swap_is_identity() {
        let base = Tableau::zero_state(2);
        let mut tab = base.clone();
        tab.h(0); // break the symmetry so swap is actually observable
        let after_h = tab.clone();
        tab.swap(0, 1);
        tab.swap(0, 1);
        assert_eq!(tab.x, after_h.x);
        assert_eq!(tab.z, after_h.z);
        assert_eq!(tab.r, after_h.r);
    }
}
