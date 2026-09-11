//! OpenQASM 2.0 import/export for [`QuantumProgram`] (lane Q2, register `D-QN-1`/
//! `D-QN-6`).
//!
//! CONCEPT:EG-KG.compute.quantum-qasm-interop — bidirectional `QuantumProgram` <->
//! OpenQASM 2.0 text, hand-rolled (zero new Cargo dependencies), over the bounded
//! subset the IR can express. See `crate::lib`'s
//! `CONCEPT:EG-KG.compute.quantum-circuit-ir` for the IR this interoperates with.
//!
//! This is a hand-rolled lexer/parser for a **bounded subset** of OpenQASM 2.0 —
//! exactly what [`crate::ir::GateKind`] can express, plus `measure`/`reset`/
//! `barrier`/`qreg`/`creg`. It is not a general OpenQASM 2.0 toolchain: anything
//! outside that subset is a typed [`QasmError`], never a silent drop or
//! mistranslation, matching the same "reject, don't silently mis-simulate" posture
//! `GateInstruction::is_clifford` documents for the stabilizer backend.
//!
//! # Supported subset
//!
//! - Exactly **one** `qreg` declaration (this IR has one flat `n_qubits` address
//!   space, not named/multiple quantum registers) and any number of `creg`
//!   declarations.
//! - Every zero-parameter, zero-control [`GateKind`] (`id`, `x`, `y`, `z`, `h`, `s`,
//!   `sdg`, `t`, `tdg`, `swap`), every one-parameter gate (`rx`, `ry`, `rz`, `u1` for
//!   [`GateKind::Phase`], `rxx`, `ryy`, `rzz`), and the standard `qelib1.inc`
//!   single-positive-control forms `cx`/`cy`/`cz`/`ch`/`crz`/`cu1` (mapping to
//!   [`GateKind::X`]/[`Y`]/[`Z`]/[`H`]/[`Rz`]/[`Phase`] plus one
//!   [`crate::ir::ControlQubit`] with [`crate::ir::ControlState::One`] — matching
//!   this IR's own design, where a control is a modifier, not baked into the gate
//!   name).
//! - `measure q[i] -> c[j];`, `reset q[i];`, `barrier q[i],q[j],...;`.
//! - `gate NAME(...) ... { ... }` definition blocks are recognized and **skipped**
//!   (their body is never interpreted) — only invocations matter, and an invocation
//!   of an unrecognized name is rejected regardless of whether it was "defined".
//!
//! # Explicitly unsupported (typed error, not silently dropped)
//!
//! - Any gate name outside the table above (`u2`, `u3`, `ccx`, `cswap`, a
//!   user-defined custom gate, ...) → [`QasmError::UnsupportedGate`].
//! - A gate with 2+ controls, or a negative-polarity control (no direct
//!   `qelib1.inc` representation) → [`QasmError::UnsupportedForExport`] on export;
//!   on import these simply never parse as controlled forms because this parser
//!   never emits/recognizes a multi-control call syntax for `qelib1.inc`'s two-qubit
//!   gate names.
//! - A symbolic [`crate::ir::ParamValue::Symbol`] parameter — OpenQASM 2.0 gate
//!   calls take literal numeric expressions only — → [`QasmError::UnsupportedForExport`].
//! - Classically-controlled `if (...) ...;` → [`QasmError::UnsupportedConstruct`].
//! - More than one `qreg` declaration → [`QasmError::UnsupportedConstruct`].
//! - A reference to an undeclared register, an unmatched paren/bracket, a
//!   non-numeric gate parameter, or any other malformed text →
//!   [`QasmError::Parse`]/[`QasmError::UnknownRegister`].

use crate::ir::IrValidationError;

mod export;
mod import;

/// Errors from [`to_qasm2`]/[`from_qasm2`].
#[derive(Debug, thiserror::Error)]
pub enum QasmError {
    /// A malformed statement: bad syntax, unmatched delimiter, non-numeric
    /// parameter, wrong argument count, etc. `line` is best-effort (the source line
    /// the offending statement started on).
    #[error("line {line}: {message}")]
    Parse { line: usize, message: String },
    /// A gate name this parser does not recognize at all (outside the supported
    /// subset — e.g. `u3`, `ccx`, a user-defined custom gate).
    #[error("gate '{0}' is not in the supported OpenQASM 2.0 subset")]
    UnsupportedGate(String),
    /// A recognized-but-unsupported top-level construct (classically-controlled
    /// `if`, an unsupported `OPENQASM` version, more than one `qreg`, ...).
    #[error("unsupported OpenQASM construct: {0}")]
    UnsupportedConstruct(String),
    /// A qubit/creg reference to a register name that was never declared.
    #[error("reference to undeclared register '{0}'")]
    UnknownRegister(String),
    /// An IR value this parser CAN build syntactically valid `QuantumProgram`
    /// instructions for, but which has no OpenQASM 2.0 textual representation this
    /// exporter supports (a symbolic parameter, a 2+-control gate, a negative
    /// control, a `Custom` gate, an arity mismatch).
    #[error("cannot export to OpenQASM 2.0: {0}")]
    UnsupportedForExport(String),
    /// The parsed program failed [`QuantumProgram::validate`] (e.g. a qubit index
    /// out of range, an unknown classical register referenced by `measure`).
    #[error("parsed program failed IR validation: {0}")]
    Invalid(#[from] IrValidationError),
}

pub use export::to_qasm2;
pub use import::from_qasm2;
