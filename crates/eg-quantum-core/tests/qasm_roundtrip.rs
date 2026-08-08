//! OpenQASM 2.0 import/export round-trip tests (lane Q2, register `D-QN-1`/
//! `D-QN-6`). Mirrors the Bell/GHZ shapes `crates/eg-quantum-sim/tests/smoke.rs`
//! builds against the real `QuantumBackend` trait, but constructed directly against
//! the IR here since `eg-quantum-core` cannot depend on `eg-quantum-sim` (DAG
//! direction, see `AGENTS.md`'s "Module Structure").

use eg_quantum_core::qasm::{from_qasm2, to_qasm2, QasmError};
use eg_quantum_core::{
    ClassicalBitRef, ClassicalRegister, ControlQubit, ControlState, GateInstruction, GateKind,
    Instruction, ParamValue, QuantumProgram, IR_VERSION,
};

fn gate(kind: GateKind, qubits: &[u32]) -> Instruction {
    Instruction::Gate(GateInstruction {
        gate: kind,
        qubits: qubits.to_vec(),
        controls: vec![],
        params: vec![],
    })
}

fn gate1p(kind: GateKind, qubits: &[u32], param: f64) -> Instruction {
    Instruction::Gate(GateInstruction {
        gate: kind,
        qubits: qubits.to_vec(),
        controls: vec![],
        params: vec![ParamValue::Literal(param)],
    })
}

fn cnot(control: u32, target: u32) -> Instruction {
    Instruction::Gate(GateInstruction {
        gate: GateKind::X,
        qubits: vec![target],
        controls: vec![ControlQubit {
            qubit: control,
            state: ControlState::One,
        }],
        params: vec![],
    })
}

fn measure(qubit: u32, reg: &str, index: u32) -> Instruction {
    Instruction::Measure {
        qubit,
        classical_bit: ClassicalBitRef {
            register: reg.to_string(),
            index,
        },
    }
}

fn bell_program() -> QuantumProgram {
    QuantumProgram {
        ir_version: IR_VERSION,
        n_qubits: 2,
        classical_registers: vec![ClassicalRegister {
            name: "c".to_string(),
            n_bits: 2,
        }],
        parameters: vec![],
        instructions: vec![
            gate(GateKind::H, &[0]),
            cnot(0, 1),
            measure(0, "c", 0),
            measure(1, "c", 1),
        ],
        metadata: Default::default(),
    }
}

fn ghz_program() -> QuantumProgram {
    QuantumProgram {
        ir_version: IR_VERSION,
        n_qubits: 3,
        classical_registers: vec![ClassicalRegister {
            name: "c".to_string(),
            n_bits: 3,
        }],
        parameters: vec![],
        instructions: vec![
            gate(GateKind::H, &[0]),
            cnot(0, 1),
            cnot(0, 2),
            measure(0, "c", 0),
            measure(1, "c", 1),
            measure(2, "c", 2),
        ],
        metadata: Default::default(),
    }
}

/// Touches every gate kind this exporter/parser supports: every zero-param
/// single-qubit gate, every one-param single-qubit gate, every two-qubit intrinsic
/// (Swap/Rxx/Ryy/Rzz), and every supported single-positive-control form
/// (cx/cy/cz/ch/crz/cu1), plus measure/reset/barrier.
fn kitchen_sink_supported_program() -> QuantumProgram {
    QuantumProgram {
        ir_version: IR_VERSION,
        n_qubits: 4,
        classical_registers: vec![ClassicalRegister {
            name: "c".to_string(),
            n_bits: 4,
        }],
        parameters: vec![],
        instructions: vec![
            gate(GateKind::Id, &[0]),
            gate(GateKind::X, &[0]),
            gate(GateKind::Y, &[0]),
            gate(GateKind::Z, &[0]),
            gate(GateKind::H, &[0]),
            gate(GateKind::S, &[0]),
            gate(GateKind::Sdg, &[0]),
            gate(GateKind::T, &[0]),
            gate(GateKind::Tdg, &[0]),
            gate(GateKind::Swap, &[0, 1]),
            gate1p(GateKind::Rx, &[1], 0.5),
            gate1p(GateKind::Ry, &[1], -0.25),
            gate1p(GateKind::Rz, &[1], std::f64::consts::FRAC_PI_2),
            gate1p(GateKind::Phase, &[1], 0.75),
            gate1p(GateKind::Rxx, &[1, 2], 0.2),
            gate1p(GateKind::Ryy, &[1, 2], 0.3),
            gate1p(GateKind::Rzz, &[1, 2], 0.4),
            Instruction::Gate(GateInstruction {
                gate: GateKind::X,
                qubits: vec![1],
                controls: vec![ControlQubit {
                    qubit: 0,
                    state: ControlState::One,
                }],
                params: vec![],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::Y,
                qubits: vec![1],
                controls: vec![ControlQubit {
                    qubit: 0,
                    state: ControlState::One,
                }],
                params: vec![],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::Z,
                qubits: vec![1],
                controls: vec![ControlQubit {
                    qubit: 0,
                    state: ControlState::One,
                }],
                params: vec![],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::H,
                qubits: vec![1],
                controls: vec![ControlQubit {
                    qubit: 0,
                    state: ControlState::One,
                }],
                params: vec![],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::Rz,
                qubits: vec![1],
                controls: vec![ControlQubit {
                    qubit: 0,
                    state: ControlState::One,
                }],
                params: vec![ParamValue::Literal(0.6)],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::Phase,
                qubits: vec![1],
                controls: vec![ControlQubit {
                    qubit: 0,
                    state: ControlState::One,
                }],
                params: vec![ParamValue::Literal(0.9)],
            }),
            Instruction::Barrier {
                qubits: vec![0, 1, 2, 3],
            },
            Instruction::Reset { qubit: 3 },
            measure(0, "c", 0),
            measure(1, "c", 1),
            measure(2, "c", 2),
            measure(3, "c", 3),
        ],
        metadata: Default::default(),
    }
}

/// Compare everything a QASM round trip can and should preserve — the substantive
/// circuit content — while deliberately excluding `metadata`. `metadata.source` is
/// documented (`ir.rs`) as provenance only, "never interpreted by the planner or a
/// backend", and `from_qasm2` legitimately stamps its own provenance
/// (`"openqasm2-import"`) rather than fabricating whatever the original program's
/// metadata happened to say.
fn assert_same_circuit(original: &QuantumProgram, reimported: &QuantumProgram) {
    assert_eq!(original.ir_version, reimported.ir_version);
    assert_eq!(original.n_qubits, reimported.n_qubits);
    assert_eq!(original.classical_registers, reimported.classical_registers);
    assert_eq!(original.parameters, reimported.parameters);
    assert_eq!(original.instructions, reimported.instructions);
}

#[test]
fn bell_program_round_trips_through_qasm2() {
    let program = bell_program();
    let qasm = to_qasm2(&program).expect("export");
    assert!(qasm.contains("OPENQASM 2.0;"));
    assert!(qasm.contains("qreg q[2];"));
    assert!(qasm.contains("creg c[2];"));
    assert!(qasm.contains("h q[0];"));
    assert!(qasm.contains("cx q[0],q[1];"));
    assert!(qasm.contains("measure q[0] -> c[0];"));
    assert!(qasm.contains("measure q[1] -> c[1];"));

    let reimported = from_qasm2(&qasm).expect("import");
    assert_same_circuit(&program, &reimported);
    assert_eq!(
        reimported.metadata.source.as_deref(),
        Some("openqasm2-import")
    );
}

#[test]
fn ghz_program_round_trips_through_qasm2() {
    let program = ghz_program();
    let qasm = to_qasm2(&program).expect("export");
    let reimported = from_qasm2(&qasm).expect("import");
    assert_same_circuit(&program, &reimported);
}

#[test]
fn kitchen_sink_supported_gates_round_trip_through_qasm2() {
    let program = kitchen_sink_supported_program();
    let qasm = to_qasm2(&program).expect("export");
    let reimported = from_qasm2(&qasm).expect("import");
    assert_same_circuit(&program, &reimported);
}

#[test]
fn export_then_reimport_is_idempotent_on_the_qasm_text() {
    // Re-exporting the reimported program must produce byte-identical text — proves
    // there is no silent drift hiding behind the round trip.
    let program = ghz_program();
    let qasm1 = to_qasm2(&program).expect("export 1");
    let reimported = from_qasm2(&qasm1).expect("import");
    let qasm2 = to_qasm2(&reimported).expect("export 2");
    assert_eq!(qasm1, qasm2);
}

// ── Import rejection: unsupported constructs ───────────────────────────────────

const HEADER: &str = "OPENQASM 2.0;\ninclude \"qelib1.inc\";\nqreg q[3];\ncreg c[3];\n";

#[test]
fn unsupported_gate_u3_is_rejected_on_import() {
    let src = format!("{HEADER}u3(0.1,0.2,0.3) q[0];\n");
    let err = from_qasm2(&src).expect_err("u3 must be rejected");
    match err {
        QasmError::UnsupportedGate(name) => assert_eq!(name, "u3"),
        other => panic!("expected UnsupportedGate, got {other:?}"),
    }
}

#[test]
fn unsupported_gate_ccx_is_rejected_on_import() {
    let src = format!("{HEADER}ccx q[0],q[1],q[2];\n");
    let err = from_qasm2(&src).expect_err("ccx must be rejected");
    match err {
        QasmError::UnsupportedGate(name) => assert_eq!(name, "ccx"),
        other => panic!("expected UnsupportedGate, got {other:?}"),
    }
}

#[test]
fn custom_gate_definition_body_is_skipped_but_unsupported_call_still_rejected() {
    // A `gate` definition block must not smuggle an unsupported name into the
    // supported table just because it was "defined" — only invocations are
    // recognized, against the fixed built-in table.
    let src = format!("{HEADER}gate bell a,b {{ h a; cx a,b; }}\nbell q[0],q[1];\n");
    let err = from_qasm2(&src).expect_err("custom-defined gate call must be rejected");
    match err {
        QasmError::UnsupportedGate(name) => assert_eq!(name, "bell"),
        other => panic!("expected UnsupportedGate, got {other:?}"),
    }
}

#[test]
fn classically_controlled_if_is_rejected_on_import() {
    let src = format!("{HEADER}if (c==1) x q[0];\n");
    let err = from_qasm2(&src).expect_err("classically-controlled if must be rejected");
    assert!(matches!(err, QasmError::UnsupportedConstruct(_)));
}

#[test]
fn multiple_qreg_declarations_are_rejected_on_import() {
    let src = "OPENQASM 2.0;\ninclude \"qelib1.inc\";\nqreg q[2];\nqreg r[2];\n";
    let err = from_qasm2(src).expect_err("second qreg must be rejected");
    assert!(matches!(err, QasmError::UnsupportedConstruct(_)));
}

// ── Import rejection: malformed text ────────────────────────────────────────────

#[test]
fn malformed_qasm_undeclared_register_is_a_typed_error() {
    let src = format!("{HEADER}cx q[0],r[1];\n");
    let err = from_qasm2(&src).expect_err("reference to undeclared register 'r' must be rejected");
    match err {
        QasmError::UnknownRegister(name) => assert_eq!(name, "r"),
        other => panic!("expected UnknownRegister, got {other:?}"),
    }
}

#[test]
fn malformed_qasm_unmatched_paren_is_a_typed_parse_error() {
    let src = format!("{HEADER}rx(0.5 q[0];\n");
    let err = from_qasm2(&src).expect_err("unmatched paren must be rejected");
    assert!(matches!(err, QasmError::Parse { .. }));
}

#[test]
fn malformed_qasm_non_numeric_parameter_is_a_typed_parse_error() {
    let src = format!("{HEADER}rx(theta) q[0];\n");
    let err = from_qasm2(&src).expect_err("symbolic/non-numeric gate parameter must be rejected");
    assert!(matches!(err, QasmError::Parse { .. }));
}

#[test]
fn missing_header_is_a_typed_parse_error() {
    let src = "qreg q[1];\nh q[0];\n";
    let err = from_qasm2(src).expect_err("missing OPENQASM header must be rejected");
    assert!(matches!(err, QasmError::Parse { .. }));
}

#[test]
fn qubit_out_of_range_fails_ir_validation() {
    let src = "OPENQASM 2.0;\ninclude \"qelib1.inc\";\nqreg q[1];\nh q[5];\n";
    let err = from_qasm2(src).expect_err("out-of-range qubit must be rejected");
    assert!(matches!(err, QasmError::Invalid(_)));
}

// ── Export rejection: IR content with no OpenQASM 2.0 representation ───────────

#[test]
fn export_rejects_symbolic_parameter() {
    let program = QuantumProgram {
        ir_version: IR_VERSION,
        n_qubits: 1,
        classical_registers: vec![],
        parameters: vec![eg_quantum_core::Parameter {
            name: "theta".to_string(),
            default: None,
        }],
        instructions: vec![Instruction::Gate(GateInstruction {
            gate: GateKind::Rz,
            qubits: vec![0],
            controls: vec![],
            params: vec![ParamValue::Symbol("theta".to_string())],
        })],
        metadata: Default::default(),
    };
    let err = to_qasm2(&program).expect_err("symbolic parameter must be rejected on export");
    assert!(matches!(err, QasmError::UnsupportedForExport(_)));
}

#[test]
fn export_rejects_two_control_gate() {
    let program = QuantumProgram {
        ir_version: IR_VERSION,
        n_qubits: 3,
        classical_registers: vec![],
        parameters: vec![],
        instructions: vec![Instruction::Gate(GateInstruction {
            gate: GateKind::X,
            qubits: vec![2],
            controls: vec![
                ControlQubit {
                    qubit: 0,
                    state: ControlState::One,
                },
                ControlQubit {
                    qubit: 1,
                    state: ControlState::One,
                },
            ],
            params: vec![],
        })],
        metadata: Default::default(),
    };
    let err = to_qasm2(&program).expect_err("2-control gate must be rejected on export");
    assert!(matches!(err, QasmError::UnsupportedForExport(_)));
}

#[test]
fn export_rejects_negative_polarity_control() {
    let program = QuantumProgram {
        ir_version: IR_VERSION,
        n_qubits: 2,
        classical_registers: vec![],
        parameters: vec![],
        instructions: vec![Instruction::Gate(GateInstruction {
            gate: GateKind::X,
            qubits: vec![1],
            controls: vec![ControlQubit {
                qubit: 0,
                state: ControlState::Zero,
            }],
            params: vec![],
        })],
        metadata: Default::default(),
    };
    let err = to_qasm2(&program).expect_err("negative-polarity control must be rejected on export");
    assert!(matches!(err, QasmError::UnsupportedForExport(_)));
}

#[test]
fn export_rejects_custom_gate() {
    let program = QuantumProgram {
        ir_version: IR_VERSION,
        n_qubits: 1,
        classical_registers: vec![],
        parameters: vec![],
        instructions: vec![gate(GateKind::Custom("my_exotic_gate".to_string()), &[0])],
        metadata: Default::default(),
    };
    let err = to_qasm2(&program).expect_err("Custom gate must be rejected on export");
    assert!(matches!(err, QasmError::UnsupportedForExport(_)));
}

#[test]
fn export_validates_the_program_first() {
    // A structurally-invalid program (qubit out of range) must be rejected by
    // export up front, not turned into OpenQASM text that fails to reimport.
    let program = QuantumProgram {
        ir_version: IR_VERSION,
        n_qubits: 1,
        classical_registers: vec![],
        parameters: vec![],
        instructions: vec![gate(GateKind::X, &[7])],
        metadata: Default::default(),
    };
    let err = to_qasm2(&program).expect_err("out-of-range qubit must fail export validation");
    assert!(matches!(err, QasmError::Invalid(_)));
}
