//! Serde round-trip tests for `QuantumProgram` across every `Instruction`/`GateKind`/
//! `ControlState`/`ParamValue` variant, plus `validate()`/`is_clifford()`/`depth()`
//! sanity checks. The IR is only "stable" if it survives a JSON round trip
//! byte-for-shape-identical — this is what proves that, not just that it compiles.

mod common;

use eg_quantum_core::{
    ClassicalBitRef, ClassicalRegister, ControlQubit, ControlState, GateInstruction, GateKind,
    Instruction, IrValidationError, ParamValue, Parameter, ProgramMetadata, QuantumProgram,
    IR_VERSION,
};

fn full_program() -> QuantumProgram {
    QuantumProgram {
        ir_version: IR_VERSION,
        n_qubits: 4,
        classical_registers: vec![
            ClassicalRegister {
                name: "c".to_string(),
                n_bits: 2,
            },
            ClassicalRegister {
                name: "aux".to_string(),
                n_bits: 1,
            },
        ],
        parameters: vec![
            Parameter {
                name: "theta".to_string(),
                default: Some(std::f64::consts::FRAC_PI_2),
            },
            Parameter {
                name: "phi".to_string(),
                default: None,
            },
        ],
        instructions: vec![
            Instruction::Gate(GateInstruction {
                gate: GateKind::Id,
                qubits: vec![0],
                controls: vec![],
                params: vec![],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::X,
                qubits: vec![0],
                controls: vec![],
                params: vec![],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::Y,
                qubits: vec![0],
                controls: vec![],
                params: vec![],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::Z,
                qubits: vec![0],
                controls: vec![],
                params: vec![],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::H,
                qubits: vec![1],
                controls: vec![],
                params: vec![],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::S,
                qubits: vec![1],
                controls: vec![],
                params: vec![],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::Sdg,
                qubits: vec![1],
                controls: vec![],
                params: vec![],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::T,
                qubits: vec![1],
                controls: vec![],
                params: vec![],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::Tdg,
                qubits: vec![1],
                controls: vec![],
                params: vec![],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::Swap,
                qubits: vec![2, 3],
                controls: vec![],
                params: vec![],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::Rx,
                qubits: vec![2],
                controls: vec![],
                params: vec![ParamValue::Literal(0.5)],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::Ry,
                qubits: vec![2],
                controls: vec![],
                params: vec![ParamValue::Symbol("theta".to_string())],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::Rz,
                qubits: vec![2],
                controls: vec![],
                params: vec![ParamValue::Symbol("phi".to_string())],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::Rzz,
                qubits: vec![2, 3],
                controls: vec![],
                params: vec![ParamValue::Literal(0.1)],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::Rxx,
                qubits: vec![2, 3],
                controls: vec![],
                params: vec![ParamValue::Literal(0.2)],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::Ryy,
                qubits: vec![2, 3],
                controls: vec![],
                params: vec![ParamValue::Literal(0.3)],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::Phase,
                qubits: vec![3],
                controls: vec![],
                params: vec![ParamValue::Literal(0.4)],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::X,
                qubits: vec![1],
                controls: vec![
                    ControlQubit {
                        qubit: 0,
                        state: ControlState::One,
                    },
                    ControlQubit {
                        qubit: 2,
                        state: ControlState::Zero,
                    },
                ],
                params: vec![],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::Custom("my_exotic_gate".to_string()),
                qubits: vec![0, 1, 2],
                controls: vec![],
                params: vec![],
            }),
            Instruction::Barrier {
                qubits: vec![0, 1, 2, 3],
            },
            Instruction::Reset { qubit: 0 },
            Instruction::Measure {
                qubit: 1,
                classical_bit: ClassicalBitRef {
                    register: "c".to_string(),
                    index: 0,
                },
            },
            Instruction::Measure {
                qubit: 2,
                classical_bit: ClassicalBitRef {
                    register: "aux".to_string(),
                    index: 0,
                },
            },
        ],
        metadata: ProgramMetadata {
            name: Some("kitchen-sink".to_string()),
            source: Some("hand-built".to_string()),
        },
    }
}

#[test]
fn round_trips_through_json_byte_identical_shape() {
    let program = full_program();
    let json = serde_json::to_string_pretty(&program).expect("serialize");
    let restored: QuantumProgram = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(
        program, restored,
        "round trip must reproduce an equal value"
    );

    // Re-serializing the restored value must produce byte-identical JSON — proves
    // there is no silent normalization/loss hiding behind `PartialEq`.
    let json2 = serde_json::to_string_pretty(&restored).expect("serialize again");
    assert_eq!(json, json2);
}

#[test]
fn validate_accepts_well_formed_program() {
    assert!(full_program().validate().is_ok());
}

#[test]
fn validate_rejects_out_of_range_qubit() {
    let mut p = common::bell_circuit();
    p.instructions.push(Instruction::Gate(GateInstruction {
        gate: GateKind::X,
        qubits: vec![99],
        controls: vec![],
        params: vec![],
    }));
    assert_eq!(
        p.validate(),
        Err(IrValidationError::QubitOutOfRange {
            index: 99,
            n_qubits: 2
        })
    );
}

#[test]
fn validate_rejects_unknown_classical_register() {
    let mut p = common::bell_circuit();
    p.instructions.push(Instruction::Measure {
        qubit: 0,
        classical_bit: ClassicalBitRef {
            register: "does_not_exist".to_string(),
            index: 0,
        },
    });
    assert_eq!(
        p.validate(),
        Err(IrValidationError::UnknownClassicalRegister {
            register: "does_not_exist".to_string()
        })
    );
}

#[test]
fn validate_rejects_classical_bit_out_of_range() {
    let mut p = common::bell_circuit();
    p.instructions.push(Instruction::Measure {
        qubit: 0,
        classical_bit: ClassicalBitRef {
            register: "c".to_string(),
            index: 5,
        },
    });
    assert_eq!(
        p.validate(),
        Err(IrValidationError::ClassicalBitOutOfRange {
            register: "c".to_string(),
            index: 5,
            n_bits: 2
        })
    );
}

#[test]
fn validate_rejects_unknown_parameter_symbol() {
    let mut p = common::bell_circuit();
    p.instructions.push(Instruction::Gate(GateInstruction {
        gate: GateKind::Rz,
        qubits: vec![0],
        controls: vec![],
        params: vec![ParamValue::Symbol("undeclared".to_string())],
    }));
    assert_eq!(
        p.validate(),
        Err(IrValidationError::UnknownParameter(
            "undeclared".to_string()
        ))
    );
}

#[test]
fn validate_rejects_future_ir_version() {
    let mut p = common::bell_circuit();
    p.ir_version = IR_VERSION + 1;
    assert_eq!(
        p.validate(),
        Err(IrValidationError::UnsupportedIrVersion {
            found: IR_VERSION + 1,
            understood: IR_VERSION
        })
    );
}

#[test]
fn bell_circuit_is_clifford() {
    assert!(common::bell_circuit().is_clifford());
}

#[test]
fn kitchen_sink_program_is_not_clifford() {
    // Contains Rx/Ry/Rz/... continuous-parameter gates and a 2-control X — either
    // alone is enough to make the whole circuit non-Clifford.
    assert!(!full_program().is_clifford());
}

#[test]
fn depth_counts_sequential_layers_on_shared_qubits() {
    // H(0) -> layer 1. CX(control 0, target 1) depends on qubit 0 (layer 1) and
    // qubit 1 (layer 0) -> layer 2 for both qubits. measure(0) depends on qubit 0's
    // last layer (2) -> layer 3. measure(1) depends on qubit 1's last layer, which is
    // STILL 2 (unaffected by measure(0)) -> also layer 3. Max layer = 3.
    let p = common::bell_circuit();
    assert_eq!(p.depth(), 3);
}

#[test]
fn depth_is_zero_for_empty_program() {
    let p = QuantumProgram {
        ir_version: IR_VERSION,
        n_qubits: 1,
        classical_registers: vec![],
        parameters: vec![],
        instructions: vec![],
        metadata: Default::default(),
    };
    assert_eq!(p.depth(), 0);
    assert!(p.is_clifford()); // vacuously
}

#[test]
fn circuit_hash_is_deterministic_and_content_sensitive() {
    let p1 = common::bell_circuit();
    let p2 = common::bell_circuit();
    assert_eq!(
        p1.circuit_hash().unwrap(),
        p2.circuit_hash().unwrap(),
        "same circuit -> same hash"
    );

    let mut p3 = common::bell_circuit();
    p3.metadata.name = Some("renamed".to_string());
    assert_ne!(
        p1.circuit_hash().unwrap(),
        p3.circuit_hash().unwrap(),
        "even a metadata-only change must change the content hash"
    );
}
