//! `eg-quantum-sim` -- minimal pure-Rust `QuantumBackend` implementations (register
//! D-QN-1, lane Q1: "get a minimal pure-Rust statevector + stabilizer path
//! compiling and passing smoke tests").
//!
//! Two backends, both in-process, both noiseless, both `is_exact_capable: true`:
//!
//! - [`statevector::StateVectorSimulator`] ([`eg_quantum_core::backend::BackendFamily::StatevectorCpu`]):
//!   a dense `Vec<Complex64>` amplitude vector, O(2^n) space/time per gate.
//! - [`stabilizer::StabilizerSimulator`] ([`eg_quantum_core::backend::BackendFamily::Stabilizer`]):
//!   an Aaronson-Gottesman CHP tableau, O(n^2) space/time per gate -- exact for
//!   Clifford circuits, and the ONLY backend here that can represent one at all.
//!
//! # Why this crate is not a mechanical port of QuantRS2's `sim` crate
//!
//! Register D-QN-3 (the numeric-stack decision, recorded in
//! `crates/eg-quantum-core/src/lib.rs` and executed by this lane) already commits
//! this workspace to `eg-numeric`, not QuantRS2's SciRS2 stack. Inspecting the
//! actual vendoring source confirmed that commitment goes deeper than the top-level
//! `Cargo.toml` `scirs2-*` dependency lines Q0 found: even QuantRS2's single most
//! self-contained simulation file, `sim/src/statevector.rs`, imports
//! `scirs2_core::{Complex64, parallel_ops}`, `crate::scirs2_integration::{SciRS2Backend,
//! SciRS2Matrix, SciRS2Vector, BLAS}`, and `quantrs2_circuit::builder::{Circuit,
//! Simulator}` -- a full second circuit-representation type this workspace does not
//! need (Q0's `QuantumProgram` IR already fills that role) plus buffer-pool/SIMD/
//! diagnostics machinery that is exactly the kind of research/perf surface register
//! D-QN-1 charges Q1 to strip. `sim/src/stabilizer/{types,functions}.rs` are
//! similarly coupled to `scirs2_core::{ndarray, random}`. None of it is a clean
//! transplant once `scirs2_*` is stripped -- it is a rewrite either way.
//!
//! So this crate takes the position the charter's own risk section anticipates:
//! **own the control plane, borrow only what is genuinely worth borrowing.** What
//! IS carried over from QuantRS2, attributed under Apache-2.0 (see
//! `crates/eg-quantum-gates/NOTICE`), is the one piece with real, non-reimplementable
//! content -- the dense gate matrices themselves (`eg-quantum-gates`). The
//! evolution/application loop here and the stabilizer tableau are clean-room
//! implementations against `eg-quantum-core`'s IR and `eg-numeric`'s `Complex64`
//! surface: standard, textbook algorithms (statevector amplitude update by bit-index
//! pairing; the Aaronson-Gottesman CHP tableau for the stabilizer formalism), not
//! copied from any scirs2-coupled QuantRS2 source that could not compile against
//! this workspace's numeric stack anyway.
//!
//! # Scope explicitly NOT in this crate
//!
//! No noise (`RunOptions.noise_model_id` is rejected outright by both backends --
//! see each backend's `run`/`submit`), no GPU, no MPS, no trajectory sampling, no
//! distributed execution, no QuEST FFI, no hardware. Those are Q3/Q4/Q10.

use eg_quantum_core::ir::{ClassicalRegister, GateInstruction, ParamValue};
use std::collections::{BTreeMap, HashMap};

/// Errors internal to this crate's simulators, before they are mapped to
/// [`eg_quantum_core::backend::BackendError`] at each backend's trait boundary. Not
/// `Clone`: it wraps `eg_quantum_core::ir::IrValidationError`/`hash::HashError`,
/// neither of which is `Clone`.
#[derive(Debug, thiserror::Error)]
pub enum SimError {
    #[error("unbound parameter symbol '{0}' (not present in RunOptions.parameter_bindings)")]
    UnboundParameter(String),
    #[error("gate matrix error: {0}")]
    Gate(#[from] eg_quantum_gates::GateError),
    #[error("qubit arity mismatch for gate {gate:?}: expected {expected} qubit(s), got {got}")]
    ArityMismatch {
        gate: eg_quantum_core::ir::GateKind,
        expected: usize,
        got: usize,
    },
    #[error("circuit is not Clifford; the stabilizer backend cannot represent instruction {0:?}")]
    NotClifford(Box<GateInstruction>),
    #[error("unknown classical register '{0}' referenced by a measurement")]
    UnknownClassicalRegister(String),
    #[error("this backend does not support noise (RunOptions.noise_model_id was set)")]
    NoiseNotSupported,
    #[error(transparent)]
    IrValidation(#[from] eg_quantum_core::ir::IrValidationError),
    #[error(transparent)]
    Hash(#[from] eg_quantum_core::hash::HashError),
}

/// Resolve every [`ParamValue`] in a gate instruction against the caller's
/// `RunOptions.parameter_bindings`, in order.
pub(crate) fn resolve_params(
    instr: &GateInstruction,
    bindings: &BTreeMap<String, f64>,
) -> Result<Vec<f64>, SimError> {
    instr
        .params
        .iter()
        .map(|p| match p {
            ParamValue::Literal(v) => Ok(*v),
            ParamValue::Symbol(s) => bindings
                .get(s)
                .copied()
                .ok_or_else(|| SimError::UnboundParameter(s.clone())),
        })
        .collect()
}

/// Per-run classical-bit storage, shared by both simulators. Bits default to `0`
/// (per the `QuantumProgram` convention -- a classical register starts zeroed, same
/// as a real device) until a `Measure` instruction writes one. `pub` (not
/// `pub(crate)`) because it appears in the public signature of
/// `statevector::evolve`/`stabilizer::evolve`.
#[derive(Debug, Default)]
pub struct ClassicalMemory {
    bits: HashMap<(String, u32), bool>,
}

impl ClassicalMemory {
    pub fn set(&mut self, register: &str, index: u32, value: bool) {
        self.bits.insert((register.to_string(), index), value);
    }

    /// Concatenate every declared register's bits, in declaration order, MSB-first
    /// within each register (index 0 = leftmost), producing the single outcome key
    /// `QuantumResult`'s `Outcome::Counts` expects (e.g. `"00"`/`"11"` for a 2-bit
    /// register, matching the shape in `eg_quantum_core::result`'s own doc example).
    pub fn bitstring(&self, registers: &[ClassicalRegister]) -> String {
        let mut s = String::new();
        for reg in registers {
            for i in 0..reg.n_bits {
                let bit = self
                    .bits
                    .get(&(reg.name.clone(), i))
                    .copied()
                    .unwrap_or(false);
                s.push(if bit { '1' } else { '0' });
            }
        }
        s
    }
}

pub mod stabilizer;
pub mod statevector;
