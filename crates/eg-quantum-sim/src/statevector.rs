//! [`StateVectorSimulator`] -- a dense `Vec<Complex64>` statevector backend.
//!
//! Amplitude application is bit-index pairing over the state vector (the standard
//! technique: a 1-qubit gate touches pairs of indices differing only in the target
//! qubit's bit; a 2-qubit gate touches quadruples differing only in the two target
//! qubits' bits), implemented generically in `eg_numeric::complex::{apply_block2,
//! apply_block4}` and specialized here with control-qubit masking (a gate with N
//! control qubits only fires on the sub-block where every control bit matches its
//! required polarity -- this is how `GateKind::X` + one `ControlQubit` becomes CNOT
//! without a separate "CNOT" matrix, per `eg-quantum-core`'s IR design).

use eg_numeric::complex::{apply_block2, apply_block4, Complex64};
use eg_quantum_core::backend::{
    BackendCapabilities, BackendError, BackendFamily, BackendId, JobHandle, JobStatus,
    QuantumBackend, RunOptions,
};
use eg_quantum_core::ir::{ControlState, GateKind, Instruction, QuantumProgram};
use eg_quantum_core::result::{Formalism, Outcome, QuantumResult};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::{resolve_params, ClassicalMemory, SimError};

/// One coherent (unitary-only) evolution of a [`QuantumProgram`]: applies every
/// `Gate` instruction to a `|0...0>`-initialized amplitude vector and records
/// classical-bit outcomes for every `Measure` instruction it encounters, using
/// `rng` for the Born-rule collapse. Exposed directly (not only through
/// [`QuantumBackend`]) so callers that want the raw final amplitudes -- tests, or a
/// future Q6 numeric-bridge consumer -- do not have to go through `Outcome::Counts`.
pub fn evolve(
    program: &QuantumProgram,
    bindings: &BTreeMap<String, f64>,
    rng: &mut eg_numeric::random::Generator,
) -> Result<(Vec<Complex64>, ClassicalMemory), SimError> {
    let n = program.n_qubits;
    let dim = 1usize << n;
    let mut state = vec![Complex64::new(0.0, 0.0); dim];
    state[0] = Complex64::new(1.0, 0.0);
    let mut classical = ClassicalMemory::default();

    for instr in &program.instructions {
        match instr {
            Instruction::Gate(g) => apply_gate(&mut state, n, g, bindings)?,
            Instruction::Measure {
                qubit,
                classical_bit,
            } => {
                let outcome = measure_and_collapse(&mut state, n, *qubit, rng);
                classical.set(&classical_bit.register, classical_bit.index, outcome);
            }
            Instruction::Reset { qubit } => {
                // Measure (collapsing) then flip back to |0> if it came up 1 -- the
                // standard reset-via-measure-and-correct technique.
                let outcome = measure_and_collapse(&mut state, n, *qubit, rng);
                if outcome {
                    let x =
                        eg_quantum_gates::matrix1(&GateKind::X, &[]).expect("X is always defined");
                    apply_block2(&mut state, *qubit, x, |_| true);
                }
            }
            Instruction::Barrier { .. } => {} // scheduling hint only, no state effect
        }
    }
    Ok((state, classical))
}

fn control_predicate(g: &eg_quantum_core::ir::GateInstruction) -> impl Fn(usize) -> bool + '_ {
    move |idx: usize| {
        g.controls.iter().all(|ctrl| {
            let bit = (idx >> ctrl.qubit) & 1 == 1;
            match ctrl.state {
                ControlState::One => bit,
                ControlState::Zero => !bit,
            }
        })
    }
}

fn apply_gate(
    state: &mut [Complex64],
    n_qubits: u32,
    g: &eg_quantum_core::ir::GateInstruction,
    bindings: &BTreeMap<String, f64>,
) -> Result<(), SimError> {
    let params = resolve_params(g, bindings)?;
    let pred = control_predicate(g);
    if eg_quantum_gates::is_single_qubit_gate(&g.gate) {
        if g.qubits.len() != 1 {
            return Err(SimError::ArityMismatch {
                gate: g.gate.clone(),
                expected: 1,
                got: g.qubits.len(),
            });
        }
        let m = eg_quantum_gates::matrix1(&g.gate, &params)?;
        apply_block2(state, g.qubits[0], m, pred);
    } else {
        if g.qubits.len() != 2 {
            return Err(SimError::ArityMismatch {
                gate: g.gate.clone(),
                expected: 2,
                got: g.qubits.len(),
            });
        }
        let m = eg_quantum_gates::matrix2(&g.gate, &params)?;
        apply_block4(state, g.qubits[0], g.qubits[1], m, pred);
    }
    let _ = n_qubits;
    Ok(())
}

/// Born-rule measurement of `qubit` in the computational basis, collapsing `state`
/// in place and returning the classical outcome.
fn measure_and_collapse(
    state: &mut [Complex64],
    _n_qubits: u32,
    qubit: u32,
    rng: &mut eg_numeric::random::Generator,
) -> bool {
    let bit = qubit;
    let mut p1 = 0.0f64;
    for (idx, amp) in state.iter().enumerate() {
        if (idx >> bit) & 1 == 1 {
            p1 += amp.norm_sqr();
        }
    }
    // Guard against floating drift pushing p1 slightly outside [0,1].
    let p1 = p1.clamp(0.0, 1.0);
    let sample = rng.uniform(0.0, 1.0, 1)[0];
    let outcome = sample < p1;
    let norm_factor = if outcome {
        p1.sqrt()
    } else {
        (1.0 - p1).sqrt()
    };
    for (idx, amp) in state.iter_mut().enumerate() {
        let bit_set = (idx >> bit) & 1 == 1;
        if bit_set != outcome {
            *amp = Complex64::new(0.0, 0.0);
        } else if norm_factor > 0.0 {
            *amp /= norm_factor;
        }
    }
    outcome
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

/// A dense-statevector [`QuantumBackend`]. In-process and synchronous: `submit`
/// computes the whole result eagerly (there is no background worker), so `poll`
/// always reports `Completed` (or `BackendError::UnknownJob`) immediately.
pub struct StateVectorSimulator {
    id: BackendId,
    max_qubits: u32,
    jobs: JobStore,
}

impl Default for StateVectorSimulator {
    fn default() -> Self {
        Self::new()
    }
}

impl StateVectorSimulator {
    pub fn new() -> Self {
        StateVectorSimulator {
            id: BackendId::from("sv-cpu"),
            // A conservative default safety ceiling for a smoke-test-scale in-process
            // dense simulator: 2^24 complex128 amplitudes is 256MiB, already a lot for
            // an unbounded caller. Not a claim about the algorithm's real limit (that
            // is host-memory-bound, see `eg_quantum_core::estimate::statevector_bytes`)
            // -- a future lane can make this configurable.
            max_qubits: 24,
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
        if program.n_qubits > self.max_qubits {
            return Err(BackendError::ResourceLimit(format!(
                "n_qubits={} exceeds this backend's max_qubits_statevector={}",
                program.n_qubits, self.max_qubits
            )));
        }
        let circuit_hash = program
            .circuit_hash()
            .map_err(|e| BackendError::InvalidProgram(e.to_string()))?;
        let shots = opts.shots.unwrap_or(1);
        let start = std::time::Instant::now();
        let mut counts: BTreeMap<String, u64> = BTreeMap::new();
        let mut peak_bytes = 0u64;
        for shot in 0..shots {
            let seed = opts.seed.unwrap_or(0).wrapping_add(shot);
            let mut rng = eg_numeric::random::Generator::new(seed);
            let (state, classical) = evolve(program, &opts.parameter_bindings, &mut rng)
                .map_err(|e| BackendError::Execution(e.to_string()))?;
            peak_bytes = peak_bytes.max((state.len() * std::mem::size_of::<Complex64>()) as u64);
            let key = classical.bitstring(&program.classical_registers);
            *counts.entry(key).or_insert(0) += 1;
        }
        let wall_time_ms = start.elapsed().as_millis() as u64;
        Ok(QuantumResult::new_exact(
            self.id.clone(),
            Formalism::Statevector,
            opts.seed,
            Some(shots),
            circuit_hash,
            wall_time_ms,
            peak_bytes,
            Outcome::Counts(counts),
        ))
    }
}

impl QuantumBackend for StateVectorSimulator {
    fn backend_id(&self) -> BackendId {
        self.id.clone()
    }

    fn family(&self) -> BackendFamily {
        BackendFamily::StatevectorCpu
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            supports_density_matrix: false,
            supports_distributed: false,
            supports_noise: false,
            supports_gpu: false,
            supports_mps: false,
            supports_stabilizer: false,
            is_exact_capable: true,
            max_qubits_statevector: Some(self.max_qubits),
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
            // Already terminal (synchronous backend) -- best-effort cancel of
            // finished work is a no-op, per QuantumBackend::cancel's own doc.
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
