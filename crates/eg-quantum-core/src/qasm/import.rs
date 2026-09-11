//! OpenQASM 2.0 parsing for the bounded quantum IR.

use crate::ir::{
    ClassicalBitRef, ClassicalRegister, ControlQubit, ControlState, GateInstruction, GateKind,
    Instruction, ParamValue, ProgramMetadata, QuantumProgram, IR_VERSION,
};

use super::QasmError;

fn parse_err(line: usize, message: impl Into<String>) -> QasmError {
    QasmError::Parse {
        line,
        message: message.into(),
    }
}

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
        .map(|part| resolve_qubit(part.trim(), line, qreg))
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
        .map(|arg| arg.trim())
        .filter(|arg| !arg.is_empty())
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
    let Some(spec) = gate_spec(name) else {
        return Err(QasmError::UnsupportedGate(name.to_string()));
    };
    match spec.shape {
        GateShape::Uncontrolled { qubits, parameters } => build_gate(
            name,
            spec.gate.clone(),
            (qubits, parameters),
            params,
            args,
            line,
            qreg,
        ),
        GateShape::Controlled { parameters } => build_controlled_gate(
            name,
            spec.gate.clone(),
            parameters,
            params,
            args,
            line,
            qreg,
        ),
    }
}

#[derive(Clone, Copy)]
enum GateShape {
    Uncontrolled { qubits: usize, parameters: usize },
    Controlled { parameters: usize },
}

struct GateSpec {
    name: &'static str,
    gate: GateKind,
    shape: GateShape,
}

static GATE_SPECS: &[GateSpec] = &[
    GateSpec {
        name: "id",
        gate: GateKind::Id,
        shape: GateShape::Uncontrolled {
            qubits: 1,
            parameters: 0,
        },
    },
    GateSpec {
        name: "x",
        gate: GateKind::X,
        shape: GateShape::Uncontrolled {
            qubits: 1,
            parameters: 0,
        },
    },
    GateSpec {
        name: "y",
        gate: GateKind::Y,
        shape: GateShape::Uncontrolled {
            qubits: 1,
            parameters: 0,
        },
    },
    GateSpec {
        name: "z",
        gate: GateKind::Z,
        shape: GateShape::Uncontrolled {
            qubits: 1,
            parameters: 0,
        },
    },
    GateSpec {
        name: "h",
        gate: GateKind::H,
        shape: GateShape::Uncontrolled {
            qubits: 1,
            parameters: 0,
        },
    },
    GateSpec {
        name: "s",
        gate: GateKind::S,
        shape: GateShape::Uncontrolled {
            qubits: 1,
            parameters: 0,
        },
    },
    GateSpec {
        name: "sdg",
        gate: GateKind::Sdg,
        shape: GateShape::Uncontrolled {
            qubits: 1,
            parameters: 0,
        },
    },
    GateSpec {
        name: "t",
        gate: GateKind::T,
        shape: GateShape::Uncontrolled {
            qubits: 1,
            parameters: 0,
        },
    },
    GateSpec {
        name: "tdg",
        gate: GateKind::Tdg,
        shape: GateShape::Uncontrolled {
            qubits: 1,
            parameters: 0,
        },
    },
    GateSpec {
        name: "swap",
        gate: GateKind::Swap,
        shape: GateShape::Uncontrolled {
            qubits: 2,
            parameters: 0,
        },
    },
    GateSpec {
        name: "rx",
        gate: GateKind::Rx,
        shape: GateShape::Uncontrolled {
            qubits: 1,
            parameters: 1,
        },
    },
    GateSpec {
        name: "ry",
        gate: GateKind::Ry,
        shape: GateShape::Uncontrolled {
            qubits: 1,
            parameters: 1,
        },
    },
    GateSpec {
        name: "rz",
        gate: GateKind::Rz,
        shape: GateShape::Uncontrolled {
            qubits: 1,
            parameters: 1,
        },
    },
    GateSpec {
        name: "u1",
        gate: GateKind::Phase,
        shape: GateShape::Uncontrolled {
            qubits: 1,
            parameters: 1,
        },
    },
    GateSpec {
        name: "rxx",
        gate: GateKind::Rxx,
        shape: GateShape::Uncontrolled {
            qubits: 2,
            parameters: 1,
        },
    },
    GateSpec {
        name: "ryy",
        gate: GateKind::Ryy,
        shape: GateShape::Uncontrolled {
            qubits: 2,
            parameters: 1,
        },
    },
    GateSpec {
        name: "rzz",
        gate: GateKind::Rzz,
        shape: GateShape::Uncontrolled {
            qubits: 2,
            parameters: 1,
        },
    },
    GateSpec {
        name: "cx",
        gate: GateKind::X,
        shape: GateShape::Controlled { parameters: 0 },
    },
    GateSpec {
        name: "cy",
        gate: GateKind::Y,
        shape: GateShape::Controlled { parameters: 0 },
    },
    GateSpec {
        name: "cz",
        gate: GateKind::Z,
        shape: GateShape::Controlled { parameters: 0 },
    },
    GateSpec {
        name: "ch",
        gate: GateKind::H,
        shape: GateShape::Controlled { parameters: 0 },
    },
    GateSpec {
        name: "crz",
        gate: GateKind::Rz,
        shape: GateShape::Controlled { parameters: 1 },
    },
    GateSpec {
        name: "cu1",
        gate: GateKind::Phase,
        shape: GateShape::Controlled { parameters: 1 },
    },
];

fn gate_spec(name: &str) -> Option<&'static GateSpec> {
    GATE_SPECS.iter().find(|spec| spec.name == name)
}

/// `expected` is `(qubits, params)`: the arity this gate must be given, kept
/// together because neither number means anything without the other.
fn build_gate(
    name: &str,
    gate: GateKind,
    expected: (usize, usize),
    params: &[f64],
    args: &[&str],
    line: usize,
    qreg: Option<&QregDecl>,
) -> Result<Instruction, QasmError> {
    let (expected_qubits, expected_params) = expected;
    validate_gate_shape(name, expected_qubits, expected_params, args, params, line)?;
    let qubits = resolve_gate_qubits(args, line, qreg)?;
    Ok(Instruction::Gate(GateInstruction {
        gate,
        qubits,
        controls: vec![],
        params: literal_params(params),
    }))
}

fn build_controlled_gate(
    name: &str,
    gate: GateKind,
    expected_params: usize,
    params: &[f64],
    args: &[&str],
    line: usize,
    qreg: Option<&QregDecl>,
) -> Result<Instruction, QasmError> {
    validate_gate_shape(name, 2, expected_params, args, params, line)?;
    let qubits = resolve_gate_qubits(args, line, qreg)?;
    Ok(Instruction::Gate(GateInstruction {
        gate,
        qubits: vec![qubits[1]],
        controls: vec![ControlQubit {
            qubit: qubits[0],
            state: ControlState::One,
        }],
        params: literal_params(params),
    }))
}

fn validate_gate_shape(
    name: &str,
    expected_qubits: usize,
    expected_params: usize,
    args: &[&str],
    params: &[f64],
    line: usize,
) -> Result<(), QasmError> {
    if args.len() != expected_qubits {
        return Err(parse_err(
            line,
            format!(
                "gate '{name}' expects {expected_qubits} qubit argument(s), got {}",
                args.len()
            ),
        ));
    }
    if params.len() != expected_params {
        return Err(parse_err(
            line,
            format!(
                "gate '{name}' expects {expected_params} parameter(s), got {}",
                params.len()
            ),
        ));
    }
    Ok(())
}

fn resolve_gate_qubits(
    args: &[&str],
    line: usize,
    qreg: Option<&QregDecl>,
) -> Result<Vec<u32>, QasmError> {
    args.iter()
        .map(|arg| resolve_qubit(arg, line, qreg))
        .collect()
}

fn literal_params(params: &[f64]) -> Vec<ParamValue> {
    params.iter().copied().map(ParamValue::Literal).collect()
}

struct ParserState {
    version_seen: bool,
    qreg: Option<QregDecl>,
    classical_registers: Vec<ClassicalRegister>,
    instructions: Vec<Instruction>,
}

impl ParserState {
    fn new() -> Self {
        Self {
            version_seen: false,
            qreg: None,
            classical_registers: Vec::new(),
            instructions: Vec::new(),
        }
    }

    fn parse_statement(&mut self, raw: &RawStatement) -> Result<(), QasmError> {
        let s = raw.text.trim();
        if s.is_empty() {
            return Ok(());
        }
        let line = raw.line;
        let keyword = leading_identifier(s);
        match keyword {
            "OPENQASM" => {
                parse_version(s, line)?;
                self.version_seen = true;
            }
            "include" | "gate" => {}
            "qreg" => {
                if self.qreg.is_some() {
                    return Err(QasmError::UnsupportedConstruct(format!(
                        "line {line}: multiple 'qreg' declarations are not supported (QuantumProgram has one flat n_qubits address space)"
                    )));
                }
                let (name, size) = parse_reg_decl(s, line, "qreg")?;
                self.qreg = Some(QregDecl { name, size });
            }
            "creg" => {
                let (name, n_bits) = parse_reg_decl(s, line, "creg")?;
                self.classical_registers
                    .push(ClassicalRegister { name, n_bits });
            }
            "if" => {
                return Err(QasmError::UnsupportedConstruct(format!(
                    "line {line}: classically-controlled 'if' is not supported"
                )));
            }
            "measure" | "reset" | "barrier" => self.push_instruction(keyword, s, line)?,
            _ => self.push_instruction("gate", s, line)?,
        }
        Ok(())
    }

    fn push_instruction(
        &mut self,
        keyword: &str,
        statement: &str,
        line: usize,
    ) -> Result<(), QasmError> {
        let instruction = match keyword {
            "measure" => parse_measure(statement, line, self.qreg.as_ref())?,
            "reset" => parse_reset(statement, line, self.qreg.as_ref())?,
            "barrier" => parse_barrier(statement, line, self.qreg.as_ref())?,
            _ => parse_gate_call(statement, line, self.qreg.as_ref())?,
        };
        self.instructions.push(instruction);
        Ok(())
    }

    fn finish(self) -> Result<QuantumProgram, QasmError> {
        if !self.version_seen {
            return Err(parse_err(0, "missing 'OPENQASM 2.0;' header"));
        }
        let qreg = self
            .qreg
            .ok_or_else(|| parse_err(0, "missing 'qreg' declaration"))?;
        let program = QuantumProgram {
            ir_version: IR_VERSION,
            n_qubits: qreg.size,
            classical_registers: self.classical_registers,
            parameters: Vec::new(),
            instructions: self.instructions,
            metadata: ProgramMetadata {
                name: None,
                source: Some("openqasm2-import".to_string()),
            },
        };
        program.validate()?;
        Ok(program)
    }
}

/// Parse OpenQASM 2.0 text into a [`QuantumProgram`]. See the module doc for the
/// supported subset and what is rejected.
pub fn from_qasm2(src: &str) -> Result<QuantumProgram, QasmError> {
    let cleaned = strip_comments(src);
    let statements = split_statements(&cleaned);
    let mut parser = ParserState::new();
    for raw in &statements {
        parser.parse_statement(raw)?;
    }
    parser.finish()
}
