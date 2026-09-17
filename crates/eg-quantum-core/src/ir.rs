//! `QuantumProgram` — the stable, versioned, serde-serializable quantum IR.
//!
//! This is the ONE representation every producer (a future QuantRS2 importer in Q2,
//! an OpenQASM importer in Q2, a hand-built circuit from an agent-utilities caller in
//! Q8) and every consumer (`estimate()`, the planner, every `QuantumBackend`) agrees
//! on. It is deliberately small and closed-vocabulary for gates (a `Custom` escape
//! hatch exists, but the planner treats it conservatively — see
//! [`GateKind::is_clifford`]).

use serde::{Deserialize, Serialize};

mod gates;
mod validation;

pub use gates::{
    CliffordGenerator, ControlQubit, ControlState, GateInstruction, GateKind, ParamValue,
};
pub use validation::IrValidationError;

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

impl QuantumProgram {
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
