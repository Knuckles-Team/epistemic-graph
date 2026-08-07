//! OpenQASM 2.0 import/export for [`QuantumProgram`] (lane Q2, register `D-QN-1`/
//! `D-QN-6`).
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

use crate::ir::{
    ClassicalBitRef, ClassicalRegister, ControlQubit, ControlState, GateInstruction, GateKind,
    Instruction, IrValidationError, ParamValue, ProgramMetadata, QuantumProgram, IR_VERSION,
};

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

fn parse_err(line: usize, message: impl Into<String>) -> QasmError {
    QasmError::Parse {
        line,
        message: message.into(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────────
// Export: QuantumProgram -> OpenQASM 2.0 text
// ─────────────────────────────────────────────────────────────────────────────────

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
    match &g.gate {
        GateKind::Id => write_1q0p(out, "id", q),
        GateKind::X => write_1q0p(out, "x", q),
        GateKind::Y => write_1q0p(out, "y", q),
        GateKind::Z => write_1q0p(out, "z", q),
        GateKind::H => write_1q0p(out, "h", q),
        GateKind::S => write_1q0p(out, "s", q),
        GateKind::Sdg => write_1q0p(out, "sdg", q),
        GateKind::T => write_1q0p(out, "t", q),
        GateKind::Tdg => write_1q0p(out, "tdg", q),
        GateKind::Swap => write_2q0p(out, "swap", q),
        GateKind::Rx => write_1q1p(out, "rx", q, &g.params),
        GateKind::Ry => write_1q1p(out, "ry", q, &g.params),
        GateKind::Rz => write_1q1p(out, "rz", q, &g.params),
        GateKind::Phase => write_1q1p(out, "u1", q, &g.params),
        GateKind::Rxx => write_2q1p(out, "rxx", q, &g.params),
        GateKind::Ryy => write_2q1p(out, "ryy", q, &g.params),
        GateKind::Rzz => write_2q1p(out, "rzz", q, &g.params),
        GateKind::Custom(name) => Err(QasmError::UnsupportedForExport(format!(
            "custom gate '{name}' has no OpenQASM 2.0 representation"
        ))),
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
    let target = g.qubits[0];
    match &g.gate {
        GateKind::X => {
            if !g.params.is_empty() {
                return Err(param_arity_err("cx", 0, g.params.len()));
            }
            out.push_str(&format!("cx q[{control}],q[{target}];\n"));
            Ok(())
        }
        GateKind::Y => {
            if !g.params.is_empty() {
                return Err(param_arity_err("cy", 0, g.params.len()));
            }
            out.push_str(&format!("cy q[{control}],q[{target}];\n"));
            Ok(())
        }
        GateKind::Z => {
            if !g.params.is_empty() {
                return Err(param_arity_err("cz", 0, g.params.len()));
            }
            out.push_str(&format!("cz q[{control}],q[{target}];\n"));
            Ok(())
        }
        GateKind::H => {
            if !g.params.is_empty() {
                return Err(param_arity_err("ch", 0, g.params.len()));
            }
            out.push_str(&format!("ch q[{control}],q[{target}];\n"));
            Ok(())
        }
        GateKind::Rz => {
            if g.params.len() != 1 {
                return Err(param_arity_err("crz", 1, g.params.len()));
            }
            let v = literal_param(&g.params[0])?;
            out.push_str(&format!("crz({v}) q[{control}],q[{target}];\n"));
            Ok(())
        }
        GateKind::Phase => {
            if g.params.len() != 1 {
                return Err(param_arity_err("cu1", 1, g.params.len()));
            }
            let v = literal_param(&g.params[0])?;
            out.push_str(&format!("cu1({v}) q[{control}],q[{target}];\n"));
            Ok(())
        }
        other => Err(QasmError::UnsupportedForExport(format!(
            "controlled '{other:?}' has no direct OpenQASM 2.0 qelib1.inc gate"
        ))),
    }
}

// ─────────────────────────────────────────────────────────────────────────────────
// Import: OpenQASM 2.0 text -> QuantumProgram
// ─────────────────────────────────────────────────────────────────────────────────

struct RawStatement {
    line: usize,
    text: String,
}

/// Strip `//` line comments (quote-aware, so the `"..."` filename in an `include`
/// statement is never mistaken for a comment start).
fn strip_comments(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    for line in src.lines() {
        let mut in_string = false;
        let mut cut: Option<usize> = None;
        let chars: Vec<(usize, char)> = line.char_indices().collect();
        for idx in 0..chars.len() {
            let (byte_pos, c) = chars[idx];
            if c == '"' {
                in_string = !in_string;
            } else if !in_string && c == '/' && idx + 1 < chars.len() && chars[idx + 1].1 == '/' {
                cut = Some(byte_pos);
                break;
            }
        }
        match cut {
            Some(pos) => out.push_str(&line[..pos]),
            None => out.push_str(line),
        }
        out.push('\n');
    }
    out
}

/// Split comment-stripped source into top-level statements. A statement normally
/// ends at a `;` outside any `{ }` block; a `gate ... { ... }` definition is
/// captured (braces included) as ONE statement so its body is never mistaken for
/// top-level statements — [`from_qasm2`] then discards it wholesale (recognized
/// gate CALLS are matched by a fixed name table, independent of any definition).
fn split_statements(src: &str) -> Vec<RawStatement> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut depth: i32 = 0;
    let mut line: usize = 1;
    let mut start_line: usize = 1;

    for c in src.chars() {
        match c {
            '\n' => {
                current.push(' ');
                line += 1;
            }
            '{' => {
                depth += 1;
                current.push(c);
            }
            '}' => {
                depth -= 1;
                current.push(c);
                if depth <= 0 {
                    depth = 0;
                    statements.push(RawStatement {
                        line: start_line,
                        text: std::mem::take(&mut current),
                    });
                    start_line = line;
                }
            }
            ';' if depth == 0 => {
                statements.push(RawStatement {
                    line: start_line,
                    text: std::mem::take(&mut current),
                });
                start_line = line;
            }
            other => current.push(other),
        }
    }
    if !current.trim().is_empty() {
        statements.push(RawStatement {
            line: start_line,
            text: current,
        });
    }
    statements
}

fn leading_identifier(s: &str) -> &str {
    let end = s
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(s.len());
    &s[..end]
}

/// Parse a `name[number]` fragment (used for both register declarations, where the
/// number is a SIZE, and register references, where it is an INDEX).
fn parse_bracket(s: &str, line: usize) -> Result<(String, u32), QasmError> {
    let s = s.trim();
    let open = s
        .find('[')
        .ok_or_else(|| parse_err(line, format!("expected 'name[N]', got '{s}'")))?;
    let close = s
        .find(']')
        .ok_or_else(|| parse_err(line, format!("unmatched '[' in '{s}'")))?;
    if close < open {
        return Err(parse_err(
            line,
            format!("malformed bracket expression '{s}'"),
        ));
    }
    let name = s[..open].trim().to_string();
    if name.is_empty() {
        return Err(parse_err(line, format!("missing register name in '{s}'")));
    }
    let num_str = s[open + 1..close].trim();
    let n: u32 = num_str.parse().map_err(|_| {
        parse_err(
            line,
            format!("expected integer index/size, got '{num_str}'"),
        )
    })?;
    let trailing = s[close + 1..].trim();
    if !trailing.is_empty() {
        return Err(parse_err(
            line,
            format!("unexpected trailing text '{trailing}' after '{s}'"),
        ));
    }
    Ok((name, n))
}

struct QregDecl {
    name: String,
    size: u32,
}

fn resolve_qubit(reg_ref: &str, line: usize, qreg: Option<&QregDecl>) -> Result<u32, QasmError> {
    let (name, idx) = parse_bracket(reg_ref, line)?;
    let qreg = qreg.ok_or_else(|| {
        parse_err(
            line,
            "qubit reference appears before any 'qreg' declaration",
        )
    })?;
    if name != qreg.name {
        return Err(QasmError::UnknownRegister(name));
    }
    Ok(idx)
}

fn parse_version(s: &str, line: usize) -> Result<(), QasmError> {
    let rest = s.strip_prefix("OPENQASM").unwrap_or(s).trim();
    if rest.starts_with('2') {
        Ok(())
    } else {
        Err(QasmError::UnsupportedConstruct(format!(
            "line {line}: unsupported OPENQASM version '{rest}' (only 2.x is supported)"
        )))
    }
}

fn parse_reg_decl(s: &str, line: usize, keyword: &str) -> Result<(String, u32), QasmError> {
    let rest = s.strip_prefix(keyword).unwrap_or(s).trim();
    parse_bracket(rest, line)
}

fn parse_measure(s: &str, line: usize, qreg: Option<&QregDecl>) -> Result<Instruction, QasmError> {
    let rest = s.strip_prefix("measure").unwrap_or(s).trim();
    let mut parts = rest.splitn(2, "->");
    let left = parts.next().unwrap_or("").trim();
    let right = parts
        .next()
        .ok_or_else(|| parse_err(line, format!("expected 'measure q[i] -> c[j]', got '{s}'")))?
        .trim();
    let qubit = resolve_qubit(left, line, qreg)?;
    let (creg_name, idx) = parse_bracket(right, line)?;
    Ok(Instruction::Measure {
        qubit,
        classical_bit: ClassicalBitRef {
            register: creg_name,
            index: idx,
        },
    })
}

fn parse_reset(s: &str, line: usize, qreg: Option<&QregDecl>) -> Result<Instruction, QasmError> {
    let rest = s.strip_prefix("reset").unwrap_or(s).trim();
    let qubit = resolve_qubit(rest, line, qreg)?;
    Ok(Instruction::Reset { qubit })
}

fn parse_barrier(s: &str, line: usize, qreg: Option<&QregDecl>) -> Result<Instruction, QasmError> {
    let rest = s.strip_prefix("barrier").unwrap_or(s).trim();
    let qubits = rest
        .split(',')
        .map(|p| resolve_qubit(p.trim(), line, qreg))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Instruction::Barrier { qubits })
}

fn parse_gate_call(
    s: &str,
    line: usize,
    qreg: Option<&QregDecl>,
) -> Result<Instruction, QasmError> {
    let name = leading_identifier(s);
    if name.is_empty() {
        return Err(parse_err(line, format!("expected gate name, got '{s}'")));
    }
    let mut rest = s[name.len()..].trim_start();
    let mut params: Vec<f64> = Vec::new();
    if let Some(stripped) = rest.strip_prefix('(') {
        let close = stripped
            .find(')')
            .ok_or_else(|| parse_err(line, format!("unmatched '(' in '{s}'")))?;
        let params_str = &stripped[..close];
        if !params_str.trim().is_empty() {
            for p in params_str.split(',') {
                let p = p.trim();
                let v: f64 = p.parse().map_err(|_| {
                    parse_err(
                        line,
                        format!("expected numeric literal parameter, got '{p}'"),
                    )
                })?;
                params.push(v);
            }
        }
        rest = stripped[close + 1..].trim_start();
    }
    let args: Vec<&str> = rest
        .split(',')
        .map(|a| a.trim())
        .filter(|a| !a.is_empty())
        .collect();

    build_gate_instruction(name, &params, &args, line, qreg)
}

fn build_gate_instruction(
    name: &str,
    params: &[f64],
    args: &[&str],
    line: usize,
    qreg: Option<&QregDecl>,
) -> Result<Instruction, QasmError> {
    let qubit_at = |i: usize| -> Result<u32, QasmError> {
        let a = args.get(i).ok_or_else(|| {
            parse_err(line, format!("gate '{name}' expects more qubit arguments"))
        })?;
        resolve_qubit(a, line, qreg)
    };
    let expect_arity = |n: usize| -> Result<(), QasmError> {
        if args.len() != n {
            Err(parse_err(
                line,
                format!(
                    "gate '{name}' expects {n} qubit argument(s), got {}",
                    args.len()
                ),
            ))
        } else {
            Ok(())
        }
    };
    let expect_params = |n: usize| -> Result<(), QasmError> {
        if params.len() != n {
            Err(parse_err(
                line,
                format!(
                    "gate '{name}' expects {n} parameter(s), got {}",
                    params.len()
                ),
            ))
        } else {
            Ok(())
        }
    };
    let no_control = |gate: GateKind, qubits: Vec<u32>, params_vec: Vec<ParamValue>| {
        Instruction::Gate(GateInstruction {
            gate,
            qubits,
            controls: vec![],
            params: params_vec,
        })
    };
    let one_control = |gate: GateKind, control: u32, target: u32, params_vec: Vec<ParamValue>| {
        Instruction::Gate(GateInstruction {
            gate,
            qubits: vec![target],
            controls: vec![ControlQubit {
                qubit: control,
                state: ControlState::One,
            }],
            params: params_vec,
        })
    };

    match name {
        "id" => {
            expect_arity(1)?;
            expect_params(0)?;
            Ok(no_control(GateKind::Id, vec![qubit_at(0)?], vec![]))
        }
        "x" => {
            expect_arity(1)?;
            expect_params(0)?;
            Ok(no_control(GateKind::X, vec![qubit_at(0)?], vec![]))
        }
        "y" => {
            expect_arity(1)?;
            expect_params(0)?;
            Ok(no_control(GateKind::Y, vec![qubit_at(0)?], vec![]))
        }
        "z" => {
            expect_arity(1)?;
            expect_params(0)?;
            Ok(no_control(GateKind::Z, vec![qubit_at(0)?], vec![]))
        }
        "h" => {
            expect_arity(1)?;
            expect_params(0)?;
            Ok(no_control(GateKind::H, vec![qubit_at(0)?], vec![]))
        }
        "s" => {
            expect_arity(1)?;
            expect_params(0)?;
            Ok(no_control(GateKind::S, vec![qubit_at(0)?], vec![]))
        }
        "sdg" => {
            expect_arity(1)?;
            expect_params(0)?;
            Ok(no_control(GateKind::Sdg, vec![qubit_at(0)?], vec![]))
        }
        "t" => {
            expect_arity(1)?;
            expect_params(0)?;
            Ok(no_control(GateKind::T, vec![qubit_at(0)?], vec![]))
        }
        "tdg" => {
            expect_arity(1)?;
            expect_params(0)?;
            Ok(no_control(GateKind::Tdg, vec![qubit_at(0)?], vec![]))
        }
        "swap" => {
            expect_arity(2)?;
            expect_params(0)?;
            let a = qubit_at(0)?;
            let b = qubit_at(1)?;
            Ok(no_control(GateKind::Swap, vec![a, b], vec![]))
        }
        "rx" => {
            expect_arity(1)?;
            expect_params(1)?;
            Ok(no_control(
                GateKind::Rx,
                vec![qubit_at(0)?],
                vec![ParamValue::Literal(params[0])],
            ))
        }
        "ry" => {
            expect_arity(1)?;
            expect_params(1)?;
            Ok(no_control(
                GateKind::Ry,
                vec![qubit_at(0)?],
                vec![ParamValue::Literal(params[0])],
            ))
        }
        "rz" => {
            expect_arity(1)?;
            expect_params(1)?;
            Ok(no_control(
                GateKind::Rz,
                vec![qubit_at(0)?],
                vec![ParamValue::Literal(params[0])],
            ))
        }
        "u1" => {
            expect_arity(1)?;
            expect_params(1)?;
            Ok(no_control(
                GateKind::Phase,
                vec![qubit_at(0)?],
                vec![ParamValue::Literal(params[0])],
            ))
        }
        "rxx" => {
            expect_arity(2)?;
            expect_params(1)?;
            let a = qubit_at(0)?;
            let b = qubit_at(1)?;
            Ok(no_control(
                GateKind::Rxx,
                vec![a, b],
                vec![ParamValue::Literal(params[0])],
            ))
        }
        "ryy" => {
            expect_arity(2)?;
            expect_params(1)?;
            let a = qubit_at(0)?;
            let b = qubit_at(1)?;
            Ok(no_control(
                GateKind::Ryy,
                vec![a, b],
                vec![ParamValue::Literal(params[0])],
            ))
        }
        "rzz" => {
            expect_arity(2)?;
            expect_params(1)?;
            let a = qubit_at(0)?;
            let b = qubit_at(1)?;
            Ok(no_control(
                GateKind::Rzz,
                vec![a, b],
                vec![ParamValue::Literal(params[0])],
            ))
        }
        "cx" => {
            expect_arity(2)?;
            expect_params(0)?;
            let c = qubit_at(0)?;
            let t = qubit_at(1)?;
            Ok(one_control(GateKind::X, c, t, vec![]))
        }
        "cy" => {
            expect_arity(2)?;
            expect_params(0)?;
            let c = qubit_at(0)?;
            let t = qubit_at(1)?;
            Ok(one_control(GateKind::Y, c, t, vec![]))
        }
        "cz" => {
            expect_arity(2)?;
            expect_params(0)?;
            let c = qubit_at(0)?;
            let t = qubit_at(1)?;
            Ok(one_control(GateKind::Z, c, t, vec![]))
        }
        "ch" => {
            expect_arity(2)?;
            expect_params(0)?;
            let c = qubit_at(0)?;
            let t = qubit_at(1)?;
            Ok(one_control(GateKind::H, c, t, vec![]))
        }
        "crz" => {
            expect_arity(2)?;
            expect_params(1)?;
            let c = qubit_at(0)?;
            let t = qubit_at(1)?;
            Ok(one_control(
                GateKind::Rz,
                c,
                t,
                vec![ParamValue::Literal(params[0])],
            ))
        }
        "cu1" => {
            expect_arity(2)?;
            expect_params(1)?;
            let c = qubit_at(0)?;
            let t = qubit_at(1)?;
            Ok(one_control(
                GateKind::Phase,
                c,
                t,
                vec![ParamValue::Literal(params[0])],
            ))
        }
        other => Err(QasmError::UnsupportedGate(other.to_string())),
    }
}

/// Parse OpenQASM 2.0 text into a [`QuantumProgram`]. See the module doc for the
/// supported subset and what is rejected.
pub fn from_qasm2(src: &str) -> Result<QuantumProgram, QasmError> {
    let cleaned = strip_comments(src);
    let statements = split_statements(&cleaned);

    let mut version_seen = false;
    let mut qreg: Option<QregDecl> = None;
    let mut classical_registers: Vec<ClassicalRegister> = Vec::new();
    let mut instructions: Vec<Instruction> = Vec::new();

    for raw in &statements {
        let s = raw.text.trim();
        if s.is_empty() {
            continue;
        }
        let line = raw.line;
        let keyword = leading_identifier(s);
        match keyword {
            "OPENQASM" => {
                parse_version(s, line)?;
                version_seen = true;
            }
            "include" => {
                // File content is not interpreted — the supported gate/instruction
                // vocabulary is fixed by this parser, independent of what the
                // included file actually defines.
            }
            "qreg" => {
                if qreg.is_some() {
                    return Err(QasmError::UnsupportedConstruct(format!(
                        "line {line}: multiple 'qreg' declarations are not supported (QuantumProgram has one flat n_qubits address space)"
                    )));
                }
                let (name, size) = parse_reg_decl(s, line, "qreg")?;
                qreg = Some(QregDecl { name, size });
            }
            "creg" => {
                let (name, n_bits) = parse_reg_decl(s, line, "creg")?;
                classical_registers.push(ClassicalRegister { name, n_bits });
            }
            "gate" => {
                // Definition block, braces included in `s` by construction of
                // `split_statements`. Body is never interpreted; skip wholesale.
            }
            "if" => {
                return Err(QasmError::UnsupportedConstruct(format!(
                    "line {line}: classically-controlled 'if' is not supported"
                )));
            }
            "measure" => instructions.push(parse_measure(s, line, qreg.as_ref())?),
            "reset" => instructions.push(parse_reset(s, line, qreg.as_ref())?),
            "barrier" => instructions.push(parse_barrier(s, line, qreg.as_ref())?),
            _ => instructions.push(parse_gate_call(s, line, qreg.as_ref())?),
        }
    }

    if !version_seen {
        return Err(parse_err(0, "missing 'OPENQASM 2.0;' header"));
    }
    let qreg = qreg.ok_or_else(|| parse_err(0, "missing 'qreg' declaration"))?;

    let program = QuantumProgram {
        ir_version: IR_VERSION,
        n_qubits: qreg.size,
        classical_registers,
        parameters: Vec::new(),
        instructions,
        metadata: ProgramMetadata {
            name: None,
            source: Some("openqasm2-import".to_string()),
        },
    };
    program.validate()?;
    Ok(program)
}
