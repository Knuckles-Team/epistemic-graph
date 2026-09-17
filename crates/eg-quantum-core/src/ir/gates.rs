//! Gate and control vocabulary for the quantum IR.

use serde::{Deserialize, Serialize};

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

/// The Clifford generator set `{Id, X, Y, Z, H, S, Sdg, Swap}` — the base gates (with no
/// controls and no parameters) a stabilizer tableau can apply. The one authority for
/// that vocabulary: [`GateInstruction::is_clifford`] and the stabilizer backend's gate
/// dispatch both read it through [`GateKind::clifford_generator`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliffordGenerator {
    Id,
    X,
    Y,
    Z,
    H,
    S,
    Sdg,
    Swap,
}

impl GateKind {
    /// This base gate as a Clifford generator, or `None` when it is not one. Every gate
    /// kind is named, so a new kind must be classified here. `GateInstruction::is_clifford`
    /// is the call site that actually accounts for controls/params — do not call this
    /// alone to decide Clifford-ness of a full instruction.
    pub fn clifford_generator(&self) -> Option<CliffordGenerator> {
        match self {
            GateKind::Id => Some(CliffordGenerator::Id),
            GateKind::X => Some(CliffordGenerator::X),
            GateKind::Y => Some(CliffordGenerator::Y),
            GateKind::Z => Some(CliffordGenerator::Z),
            GateKind::H => Some(CliffordGenerator::H),
            GateKind::S => Some(CliffordGenerator::S),
            GateKind::Sdg => Some(CliffordGenerator::Sdg),
            GateKind::Swap => Some(CliffordGenerator::Swap),
            GateKind::T
            | GateKind::Tdg
            | GateKind::Rx
            | GateKind::Ry
            | GateKind::Rz
            | GateKind::Rzz
            | GateKind::Rxx
            | GateKind::Ryy
            | GateKind::Phase
            | GateKind::Custom(_) => None,
        }
    }
}

impl CliffordGenerator {
    /// Whether this generator, with exactly ONE positive or negative control, is
    /// still Clifford. Only the controlled Paulis (CNOT/CY/CZ) qualify — a controlled
    /// `H`/`S`/`Sdg`/`Swap` (e.g. Fredkin) is NOT Clifford in general, and this
    /// function says so.
    pub fn is_clifford_single_controlled(self) -> bool {
        matches!(
            self,
            CliffordGenerator::X | CliffordGenerator::Y | CliffordGenerator::Z
        )
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
            0 => self.gate.clifford_generator().is_some(),
            1 => self
                .gate
                .clifford_generator()
                .is_some_and(CliffordGenerator::is_clifford_single_controlled),
            _ => false,
        }
    }
}
