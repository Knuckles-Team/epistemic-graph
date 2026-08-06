//! `QuantumProgram` — the stable, versioned, serde-serializable quantum IR.
//!
//! This is the ONE representation every producer (a future QuantRS2 importer in Q2,
//! an OpenQASM importer in Q2, a hand-built circuit from an agent-utilities caller in
//! Q8) and every consumer (`estimate()`, the planner, every `QuantumBackend`) agrees
//! on. It is deliberately small and closed-vocabulary for gates (a `Custom` escape
//! hatch exists, but the planner treats it conservatively — see
//! [`GateKind::is_clifford`]).

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Bump on any change that is not purely additive-and-ignorable by an older reader
/// (i.e. any change to the meaning of an existing field or variant). Purely additive
/// changes (a new optional field, a new enum variant a planner treats conservatively)
/// do not require a bump but SHOULD be noted in a doc comment at the change site.
pub const IR_VERSION: u32 = 1;

/// A complete quantum program: qubits, classical registers, symbolic parameters, and
/// the instruction stream. Round-trip tested (serde JSON) in `tests/ir_roundtrip.rs`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuantumProgram {
    /// Schema version this value was constructed against. A reader MUST reject (or
    /// explicitly migrate) an `ir_version` newer than the one it was built for —
    /// silently misinterpreting an unknown future shape is worse than refusing it.
    pub ir_version: u32,
    /// Total addressable qubits, indexed `0..n_qubits`. Every qubit index referenced
    /// by any instruction must be `< n_qubits`; `QuantumProgram::validate` checks this.
    pub n_qubits: u32,
    pub classical_registers: Vec<ClassicalRegister>,
    /// Named symbolic parameters a caller can bind at submit time (`RunOptions`,
    /// `backend.rs`) instead of baking literal angles into the IR — lets one compiled
    /// program serve many parameter bindings (e.g. a VQE/QAOA ansatz).
    pub parameters: Vec<Parameter>,
    pub instructions: Vec<Instruction>,
    #[serde(default)]
    pub metadata: ProgramMetadata,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProgramMetadata {
    /// Free-form human label; never interpreted by the planner or a backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Where this program came from (e.g. `"quantrs2-import"`, `"openqasm-import"`,
    /// `"hand-built"`) — provenance for observability (Q9), not semantics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassicalRegister {
    pub name: String,
    pub n_bits: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Parameter {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<f64>,
}

/// A reference to one classical bit inside a named `ClassicalRegister`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassicalBitRef {
    pub register: String,
    pub index: u32,
}

/// One instruction in the program. `#[serde(tag = "op")]` gives a stable, greppable
/// wire shape (`{"op": "gate", ...}`) instead of positional/untagged ambiguity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Instruction {
    Gate(GateInstruction),
    Measure {
        qubit: u32,
        classical_bit: ClassicalBitRef,
    },
    Reset {
        qubit: u32,
    },
    Barrier {
        qubits: Vec<u32>,
    },
}

impl Instruction {
    /// Every qubit this instruction touches (targets + controls). Used by
    /// `QuantumProgram::depth` and by qubit-range validation.
    pub fn touched_qubits(&self) -> Vec<u32> {
        match self {
            Instruction::Gate(g) => {
                let mut qs = g.qubits.clone();
                qs.extend(g.controls.iter().map(|c| c.qubit));
                qs
            }
            Instruction::Measure { qubit, .. } => vec![*qubit],
            Instruction::Reset { qubit } => vec![*qubit],
            Instruction::Barrier { qubits } => qubits.clone(),
        }
    }
}

/// A control qubit modifier attachable to any base gate. Kept separate from the
/// convenience two-qubit names (`Cx`/`Cy`/`Cz` etc. do NOT exist as their own
/// `GateKind` variants) so "how many controls, what polarity" is always visible to
/// the planner without a gate-name lookup table — `Cx` == `GateKind::X` with one
/// `ControlQubit { state: One, .. }`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlQubit {
    pub qubit: u32,
    pub state: ControlState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlState {
    /// Positive control — fires when the control qubit is `|1>`.
    One,
    /// Negative control — fires when the control qubit is `|0>`.
    Zero,
}

/// A gate parameter: either a literal angle/value baked into the IR, or a reference
/// to a named `Parameter` bound later (`RunOptions::parameter_bindings`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ParamValue {
    Literal(f64),
    Symbol(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GateInstruction {
    pub gate: GateKind,
    /// Target qubits, in gate-defined order (e.g. `Swap`'s two operands).
    pub qubits: Vec<u32>,
    #[serde(default)]
    pub controls: Vec<ControlQubit>,
    #[serde(default)]
    pub params: Vec<ParamValue>,
}

/// The gate vocabulary. Closed except for [`GateKind::Custom`], the deliberate escape
/// hatch for a gate this IR does not yet name (e.g. a QuantRS2 gate not worth
/// promoting into the shared vocabulary — Q2's job to grow this list, not Q0's).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateKind {
    // Clifford generators (no continuous parameter).
    Id,
    X,
    Y,
    Z,
    H,
    S,
    Sdg,
    Swap,
    // Non-Clifford, no continuous parameter.
    T,
    Tdg,
    // Parametrized rotations — non-Clifford for generic angles.
    Rx,
    Ry,
    Rz,
    Rzz,
    Rxx,
    Ryy,
    Phase,
    /// A gate this IR does not (yet) name by string identifier. The planner treats
    /// every `Custom` gate as non-Clifford, unconditionally — see
    /// [`GateKind::is_clifford`] for why that is a correctness requirement, not a
    /// missed optimization.
    Custom(String),
}

impl GateKind {
    /// Whether this base gate (with NO controls and NO parameters) belongs to the
    /// single-qubit Clifford generator set. `GateInstruction::is_clifford` is the
    /// call site that actually accounts for controls/params — do not call this alone
    /// to decide Clifford-ness of a full instruction.
    fn is_clifford_generator(&self) -> bool {
        matches!(
            self,
            GateKind::Id
                | GateKind::X
                | GateKind::Y
                | GateKind::Z
                | GateKind::H
                | GateKind::S
                | GateKind::Sdg
                | GateKind::Swap
        )
    }

    /// Whether this base gate, with exactly ONE positive or negative control, is
    /// still Clifford. Only the controlled Paulis (CNOT/CY/CZ) qualify — a controlled
    /// `H`/`S`/`Sdg`/`Swap` (e.g. Fredkin) is NOT Clifford in general, and this
    /// function says so.
    fn is_clifford_single_controlled(&self) -> bool {
        matches!(self, GateKind::X | GateKind::Y | GateKind::Z)
    }
}

impl GateInstruction {
    /// Conservative Clifford-ness check for ONE instruction.
    ///
    /// This is deliberately biased toward false negatives over false positives: it is
    /// safe (merely suboptimal — a Clifford circuit runs on a slower-than-necessary
    /// backend) to call a genuinely-Clifford instruction non-Clifford, but it is
    /// UNSAFE (wrong physics) to call a non-Clifford instruction Clifford, because
    /// planner rule R1 routes "is_clifford" circuits to a stabilizer backend that
    /// cannot represent non-Clifford states at all. Concretely:
    ///
    /// - Any nonzero `params` (a continuous rotation angle, literal or symbolic) ⇒
    ///   non-Clifford, always — even `Rz(pi/2)` is only Clifford for that ONE special
    ///   angle, and this IR has no mechanism (nor should it) to prove a symbolic
    ///   parameter binds to a Clifford angle at estimate() time.
    /// - Zero controls ⇒ Clifford iff the base gate is in the fixed generator set
    ///   `{Id, X, Y, Z, H, S, Sdg, Swap}`.
    /// - Exactly one control ⇒ Clifford iff the base gate is `X`, `Y`, or `Z` (CNOT,
    ///   CY, CZ — the only controlled-Cliffords in this vocabulary). A controlled-`H`/
    ///   `S`/`Sdg`/`Swap` is NOT Clifford and returns `false`.
    /// - Two or more controls (e.g. Toffoli = `X` + 2 controls, Fredkin = `Swap` + 1
    ///   control counted above) ⇒ non-Clifford, always.
    /// - `GateKind::Custom(_)` ⇒ non-Clifford, always (an unnamed gate's structure is
    ///   unknown to this IR).
    pub fn is_clifford(&self) -> bool {
        if !self.params.is_empty() {
            return false;
        }
        match self.controls.len() {
            0 => self.gate.is_clifford_generator(),
            1 => self.gate.is_clifford_single_controlled(),
            _ => false,
        }
    }
}

/// Errors `QuantumProgram::validate` can return.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum IrValidationError {
    #[error("qubit index {index} is out of range for n_qubits={n_qubits}")]
    QubitOutOfRange { index: u32, n_qubits: u32 },
    #[error("classical bit register '{register}' referenced but not declared")]
    UnknownClassicalRegister { register: String },
    #[error(
        "classical bit index {index} is out of range for register '{register}' (n_bits={n_bits})"
    )]
    ClassicalBitOutOfRange {
        register: String,
        index: u32,
        n_bits: u32,
    },
    #[error("parameter symbol '{0}' referenced but not declared")]
    UnknownParameter(String),
    #[error("ir_version {found} is newer than the version this crate understands ({understood})")]
    UnsupportedIrVersion { found: u32, understood: u32 },
}

impl QuantumProgram {
    /// Structural validation: every qubit/classical-bit/parameter reference resolves,
    /// and the IR version is one this crate understands. Does NOT check gate-arity
    /// (e.g. that `Swap` has exactly 2 qubits) — that is intentionally left to a
    /// future Q2 lint pass once the gate vocabulary has grown enough to make arity
    /// tables worth maintaining; Q0's job is the container shape, not gate semantics.
    pub fn validate(&self) -> Result<(), IrValidationError> {
        if self.ir_version > IR_VERSION {
            return Err(IrValidationError::UnsupportedIrVersion {
                found: self.ir_version,
                understood: IR_VERSION,
            });
        }
        let known_registers: BTreeSet<&str> = self
            .classical_registers
            .iter()
            .map(|r| r.name.as_str())
            .collect();
        let register_bits: std::collections::BTreeMap<&str, u32> = self
            .classical_registers
            .iter()
            .map(|r| (r.name.as_str(), r.n_bits))
            .collect();
        let known_params: BTreeSet<&str> =
            self.parameters.iter().map(|p| p.name.as_str()).collect();

        let check_qubit = |q: u32| -> Result<(), IrValidationError> {
            if q >= self.n_qubits {
                Err(IrValidationError::QubitOutOfRange {
                    index: q,
                    n_qubits: self.n_qubits,
                })
            } else {
                Ok(())
            }
        };

        for instr in &self.instructions {
            for q in instr.touched_qubits() {
                check_qubit(q)?;
            }
            if let Instruction::Measure { classical_bit, .. } = instr {
                if !known_registers.contains(classical_bit.register.as_str()) {
                    return Err(IrValidationError::UnknownClassicalRegister {
                        register: classical_bit.register.clone(),
                    });
                }
                let n_bits = register_bits[classical_bit.register.as_str()];
                if classical_bit.index >= n_bits {
                    return Err(IrValidationError::ClassicalBitOutOfRange {
                        register: classical_bit.register.clone(),
                        index: classical_bit.index,
                        n_bits,
                    });
                }
            }
            if let Instruction::Gate(g) = instr {
                for p in &g.params {
                    if let ParamValue::Symbol(s) = p {
                        if !known_params.contains(s.as_str()) {
                            return Err(IrValidationError::UnknownParameter(s.clone()));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Whether the WHOLE circuit is Clifford — every gate instruction is Clifford
    /// (see [`GateInstruction::is_clifford`]); `Measure`/`Reset`/`Barrier` do not
    /// affect Clifford-ness. An empty instruction stream is (vacuously) Clifford.
    pub fn is_clifford(&self) -> bool {
        self.instructions.iter().all(|i| match i {
            Instruction::Gate(g) => g.is_clifford(),
            Instruction::Measure { .. }
            | Instruction::Reset { .. }
            | Instruction::Barrier { .. } => true,
        })
    }

    /// Circuit depth: the standard greedy layering count — each instruction is
    /// placed one layer after the latest layer of any qubit it touches. `Barrier`
    /// counts as touching its listed qubits (it is a scheduling boundary, so it
    /// legitimately adds depth); an instruction touching zero qubits (should not
    /// happen post-`validate`) contributes layer 0.
    pub fn depth(&self) -> u32 {
        let mut last_layer: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
        let mut max_layer = 0u32;
        for instr in &self.instructions {
            let touched = instr.touched_qubits();
            if touched.is_empty() {
                continue;
            }
            let layer = touched
                .iter()
                .map(|q| last_layer.get(q).copied().unwrap_or(0))
                .max()
                .unwrap_or(0)
                + 1;
            for q in touched {
                last_layer.insert(q, layer);
            }
            max_layer = max_layer.max(layer);
        }
        max_layer
    }
}
