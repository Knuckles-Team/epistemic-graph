//! OpenQASM 2.0 export for the bounded quantum IR.

use crate::ir::{ControlState, GateInstruction, GateKind, Instruction, ParamValue, QuantumProgram};

use super::QasmError;

/// Serialize a [`QuantumProgram`] to OpenQASM 2.0 text.
///
/// The program is validated first ([`QuantumProgram::validate`]) — exporting an
/// already-inconsistent program (out-of-range qubit, unknown classical register)
/// would only produce OpenQASM text that fails to reimport, so it is rejected up
/// front instead.
pub fn to_qasm2(program: &QuantumProgram) -> Result<String, QasmError> {
    program.validate()?;

    let mut out = String::new();
    out.push_str("OPENQASM 2.0;\n");
    out.push_str("include \"qelib1.inc\";\n");
    // qelib1.inc does not universally define these three two-qubit rotations across
    // every revision in the wild; define them ourselves (standard, physically
    // correct decompositions) so the emitted text is self-contained. Harmless if
    // unused by this particular program's instructions.
    out.push_str("gate rxx(theta) a,b { h a; h b; cx a,b; rz(theta) b; cx a,b; h b; h a; }\n");
    out.push_str(
        "gate ryy(theta) a,b { rx(pi/2) a; rx(pi/2) b; cx a,b; rz(theta) b; cx a,b; rx(-pi/2) a; rx(-pi/2) b; }\n",
    );
    out.push_str("gate rzz(theta) a,b { cx a,b; rz(theta) b; cx a,b; }\n");
    out.push_str(&format!("qreg q[{}];\n", program.n_qubits));
    for reg in &program.classical_registers {
        out.push_str(&format!("creg {}[{}];\n", reg.name, reg.n_bits));
    }
    for instr in &program.instructions {
        write_instruction(&mut out, instr)?;
    }
    Ok(out)
}

fn write_instruction(out: &mut String, instr: &Instruction) -> Result<(), QasmError> {
    match instr {
        Instruction::Gate(g) => write_gate(out, g),
        Instruction::Measure {
            qubit,
            classical_bit,
        } => {
            out.push_str(&format!(
                "measure q[{}] -> {}[{}];\n",
                qubit, classical_bit.register, classical_bit.index
            ));
            Ok(())
        }
        Instruction::Reset { qubit } => {
            out.push_str(&format!("reset q[{qubit}];\n"));
            Ok(())
        }
        Instruction::Barrier { qubits } => {
            let list = qubits
                .iter()
                .map(|q| format!("q[{q}]"))
                .collect::<Vec<_>>()
                .join(",");
            out.push_str(&format!("barrier {list};\n"));
            Ok(())
        }
    }
}

fn literal_param(p: &ParamValue) -> Result<f64, QasmError> {
    match p {
        ParamValue::Literal(v) => Ok(*v),
        ParamValue::Symbol(s) => Err(QasmError::UnsupportedForExport(format!(
            "symbolic parameter '{s}' has no OpenQASM 2.0 representation (bind it to a literal before export)"
        ))),
    }
}

fn arity_err(gate: &str, expected: usize, got: usize) -> QasmError {
    QasmError::UnsupportedForExport(format!(
        "gate '{gate}' requires exactly {expected} qubit argument(s), got {got}"
    ))
}

fn param_arity_err(gate: &str, expected: usize, got: usize) -> QasmError {
    QasmError::UnsupportedForExport(format!(
        "gate '{gate}' requires exactly {expected} parameter(s), got {got}"
    ))
}

fn write_1q0p(out: &mut String, name: &str, qubits: &[u32]) -> Result<(), QasmError> {
    if qubits.len() != 1 {
        return Err(arity_err(name, 1, qubits.len()));
    }
    out.push_str(&format!("{name} q[{}];\n", qubits[0]));
    Ok(())
}

fn write_2q0p(out: &mut String, name: &str, qubits: &[u32]) -> Result<(), QasmError> {
    if qubits.len() != 2 {
        return Err(arity_err(name, 2, qubits.len()));
    }
    out.push_str(&format!("{name} q[{}],q[{}];\n", qubits[0], qubits[1]));
    Ok(())
}

fn write_1q1p(
    out: &mut String,
    name: &str,
    qubits: &[u32],
    params: &[ParamValue],
) -> Result<(), QasmError> {
    if qubits.len() != 1 {
        return Err(arity_err(name, 1, qubits.len()));
    }
    if params.len() != 1 {
        return Err(param_arity_err(name, 1, params.len()));
    }
    let v = literal_param(&params[0])?;
    out.push_str(&format!("{name}({v}) q[{}];\n", qubits[0]));
    Ok(())
}

fn write_2q1p(
    out: &mut String,
    name: &str,
    qubits: &[u32],
    params: &[ParamValue],
) -> Result<(), QasmError> {
    if qubits.len() != 2 {
        return Err(arity_err(name, 2, qubits.len()));
    }
    if params.len() != 1 {
        return Err(param_arity_err(name, 1, params.len()));
    }
    let v = literal_param(&params[0])?;
    out.push_str(&format!("{name}({v}) q[{}],q[{}];\n", qubits[0], qubits[1]));
    Ok(())
}

fn write_gate(out: &mut String, g: &GateInstruction) -> Result<(), QasmError> {
    match g.controls.len() {
        0 => write_uncontrolled(out, g),
        1 => {
            let ctrl = &g.controls[0];
            if ctrl.state != ControlState::One {
                return Err(QasmError::UnsupportedForExport(
                    "negative-polarity control has no direct OpenQASM 2.0 qelib1.inc gate"
                        .to_string(),
                ));
            }
            write_one_controlled(out, g, ctrl.qubit)
        }
        n => Err(QasmError::UnsupportedForExport(format!(
            "{n}-control gate has no direct OpenQASM 2.0 qelib1.inc representation"
        ))),
    }
}

fn write_uncontrolled(out: &mut String, g: &GateInstruction) -> Result<(), QasmError> {
    let q = &g.qubits;
    if let Some((name, shape)) = uncontrolled_gate_spec(&g.gate) {
        match shape {
            UncontrolledGateShape::OneQZeroP => write_1q0p(out, name, q),
            UncontrolledGateShape::TwoQZeroP => write_2q0p(out, name, q),
            UncontrolledGateShape::OneQOneP => write_1q1p(out, name, q, &g.params),
            UncontrolledGateShape::TwoQOneP => write_2q1p(out, name, q, &g.params),
        }
    } else {
        match &g.gate {
            GateKind::Custom(name) => Err(QasmError::UnsupportedForExport(format!(
                "custom gate '{name}' has no OpenQASM 2.0 representation"
            ))),
            other => Err(QasmError::UnsupportedForExport(format!(
                "gate '{other:?}' has no OpenQASM 2.0 representation"
            ))),
        }
    }
}

#[derive(Clone, Copy)]
enum UncontrolledGateShape {
    OneQZeroP,
    TwoQZeroP,
    OneQOneP,
    TwoQOneP,
}

fn uncontrolled_gate_spec(gate: &GateKind) -> Option<(&'static str, UncontrolledGateShape)> {
    match gate {
        GateKind::Id => Some(("id", UncontrolledGateShape::OneQZeroP)),
        GateKind::X => Some(("x", UncontrolledGateShape::OneQZeroP)),
        GateKind::Y => Some(("y", UncontrolledGateShape::OneQZeroP)),
        GateKind::Z => Some(("z", UncontrolledGateShape::OneQZeroP)),
        GateKind::H => Some(("h", UncontrolledGateShape::OneQZeroP)),
        GateKind::S => Some(("s", UncontrolledGateShape::OneQZeroP)),
        GateKind::Sdg => Some(("sdg", UncontrolledGateShape::OneQZeroP)),
        GateKind::T => Some(("t", UncontrolledGateShape::OneQZeroP)),
        GateKind::Tdg => Some(("tdg", UncontrolledGateShape::OneQZeroP)),
        GateKind::Swap => Some(("swap", UncontrolledGateShape::TwoQZeroP)),
        GateKind::Rx => Some(("rx", UncontrolledGateShape::OneQOneP)),
        GateKind::Ry => Some(("ry", UncontrolledGateShape::OneQOneP)),
        GateKind::Rz => Some(("rz", UncontrolledGateShape::OneQOneP)),
        GateKind::Phase => Some(("u1", UncontrolledGateShape::OneQOneP)),
        GateKind::Rxx => Some(("rxx", UncontrolledGateShape::TwoQOneP)),
        GateKind::Ryy => Some(("ryy", UncontrolledGateShape::TwoQOneP)),
        GateKind::Rzz => Some(("rzz", UncontrolledGateShape::TwoQOneP)),
        GateKind::Custom(_) => None,
    }
}

fn write_one_controlled(
    out: &mut String,
    g: &GateInstruction,
    control: u32,
) -> Result<(), QasmError> {
    if g.qubits.len() != 1 {
        return Err(QasmError::UnsupportedForExport(format!(
            "controlled '{:?}' requires exactly 1 target qubit, got {}",
            g.gate,
            g.qubits.len()
        )));
    }
    let (name, expected_params) = match &g.gate {
        GateKind::X => ("cx", 0),
        GateKind::Y => ("cy", 0),
        GateKind::Z => ("cz", 0),
        GateKind::H => ("ch", 0),
        GateKind::Rz => ("crz", 1),
        GateKind::Phase => ("cu1", 1),
        other => {
            return Err(QasmError::UnsupportedForExport(format!(
                "controlled '{other:?}' has no direct OpenQASM 2.0 qelib1.inc gate"
            )))
        }
    };
    if g.params.len() != expected_params {
        return Err(param_arity_err(name, expected_params, g.params.len()));
    }
    let target = g.qubits[0];
    if expected_params == 0 {
        out.push_str(&format!("{name} q[{control}],q[{target}];\n"));
    } else {
        let value = literal_param(&g.params[0])?;
        out.push_str(&format!("{name}({value}) q[{control}],q[{target}];\n"));
    }
    Ok(())
}
