//! Structural validation for [`super::QuantumProgram`].

use std::collections::{BTreeMap, BTreeSet};

use super::{ClassicalBitRef, Instruction, ParamValue, QuantumProgram, IR_VERSION};

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
        let register_bits: BTreeMap<&str, u32> = self
            .classical_registers
            .iter()
            .map(|r| (r.name.as_str(), r.n_bits))
            .collect();
        let known_params: BTreeSet<&str> =
            self.parameters.iter().map(|p| p.name.as_str()).collect();

        for instr in &self.instructions {
            validate_instruction(
                instr,
                self.n_qubits,
                &known_registers,
                &register_bits,
                &known_params,
            )?;
        }
        Ok(())
    }
}

fn validate_instruction<'a>(
    instr: &Instruction,
    n_qubits: u32,
    known_registers: &BTreeSet<&'a str>,
    register_bits: &BTreeMap<&'a str, u32>,
    known_params: &BTreeSet<&'a str>,
) -> Result<(), IrValidationError> {
    for qubit in instr.touched_qubits() {
        validate_qubit(qubit, n_qubits)?;
    }
    match instr {
        Instruction::Measure { classical_bit, .. } => {
            validate_measure(classical_bit, known_registers, register_bits)
        }
        Instruction::Gate(gate) => validate_gate_params(&gate.params, known_params),
        Instruction::Reset { .. } | Instruction::Barrier { .. } => Ok(()),
    }
}

fn validate_qubit(qubit: u32, n_qubits: u32) -> Result<(), IrValidationError> {
    if qubit >= n_qubits {
        Err(IrValidationError::QubitOutOfRange {
            index: qubit,
            n_qubits,
        })
    } else {
        Ok(())
    }
}

fn validate_measure<'a>(
    classical_bit: &ClassicalBitRef,
    known_registers: &BTreeSet<&'a str>,
    register_bits: &BTreeMap<&'a str, u32>,
) -> Result<(), IrValidationError> {
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
    Ok(())
}

fn validate_gate_params(
    params: &[ParamValue],
    known_params: &BTreeSet<&str>,
) -> Result<(), IrValidationError> {
    for param in params {
        if let ParamValue::Symbol(symbol) = param {
            if !known_params.contains(symbol.as_str()) {
                return Err(IrValidationError::UnknownParameter(symbol.clone()));
            }
        }
    }
    Ok(())
}
