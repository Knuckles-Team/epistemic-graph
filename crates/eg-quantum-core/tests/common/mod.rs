//! Shared fixtures for the eg-quantum-core test suite: small circuit builders and
//! stub `BackendDescriptor`s. No real numerics anywhere — Q0 ships no simulator, so
//! these stubs exist purely to give the planner something to select among.
//!
//! `mod common;` is compiled separately into EACH integration-test binary (that is
//! how `tests/*.rs` works), so any one binary that does not call every helper here
//! triggers a benign `dead_code` warning for that binary — not a real problem, just
//! `cargo test`'s per-binary compilation model. Silenced at the module level rather
//! than per-function since which helpers are "unused" differs by binary.
#![allow(dead_code)]

use eg_quantum_core::{
    BackendCapabilities, BackendDescriptor, BackendFamily, BackendId, ClassicalBitRef,
    ClassicalRegister, ControlQubit, ControlState, GateInstruction, GateKind, Instruction,
    ParamValue, QuantumProgram, IR_VERSION,
};

/// A 2-qubit Bell-state circuit: `H(0); CX(0,1); measure both`. Every gate is
/// Clifford (H with no controls; X with exactly one control), so the whole program
/// must report `is_clifford() == true`.
pub fn bell_circuit() -> QuantumProgram {
    QuantumProgram {
        ir_version: IR_VERSION,
        n_qubits: 2,
        classical_registers: vec![ClassicalRegister {
            name: "c".to_string(),
            n_bits: 2,
        }],
        parameters: vec![],
        instructions: vec![
            Instruction::Gate(GateInstruction {
                gate: GateKind::H,
                qubits: vec![0],
                controls: vec![],
                params: vec![],
            }),
            Instruction::Gate(GateInstruction {
                gate: GateKind::X,
                qubits: vec![1],
                controls: vec![ControlQubit {
                    qubit: 0,
                    state: ControlState::One,
                }],
                params: vec![],
            }),
            Instruction::Measure {
                qubit: 0,
                classical_bit: ClassicalBitRef {
                    register: "c".to_string(),
                    index: 0,
                },
            },
            Instruction::Measure {
                qubit: 1,
                classical_bit: ClassicalBitRef {
                    register: "c".to_string(),
                    index: 1,
                },
            },
        ],
        metadata: Default::default(),
    }
}

/// A non-Clifford circuit on `n_qubits` (a continuous-angle `Rx` rotation makes every
/// instance non-Clifford regardless of size), for memory-bound / placement tests.
pub fn non_clifford_circuit(n_qubits: u32) -> QuantumProgram {
    QuantumProgram {
        ir_version: IR_VERSION,
        n_qubits,
        classical_registers: vec![],
        parameters: vec![],
        instructions: vec![Instruction::Gate(GateInstruction {
            gate: GateKind::Rx,
            qubits: vec![0],
            controls: vec![],
            params: vec![ParamValue::Literal(0.3)],
        })],
        metadata: Default::default(),
    }
}

/// A non-Clifford (the leading `Rx` breaks Cliffordness so R1 never preempts R3),
/// nearest-neighbor-chain-structured circuit on `n_qubits` (`n_qubits >= 2`):
/// `Rx(0)` then `CNOT(i, i+1)` for `i in 0..n_qubits-1`. Its entangling gates form a
/// simple path in qubit-index order, so `estimate()`'s `EntanglingConnectivity`
/// classifies it `NearestNeighborChain`.
pub fn chain_entangled_circuit(n_qubits: u32) -> QuantumProgram {
    let mut instructions = vec![Instruction::Gate(GateInstruction {
        gate: GateKind::Rx,
        qubits: vec![0],
        controls: vec![],
        params: vec![ParamValue::Literal(0.3)],
    })];
    for i in 0..n_qubits.saturating_sub(1) {
        instructions.push(Instruction::Gate(GateInstruction {
            gate: GateKind::X,
            qubits: vec![i + 1],
            controls: vec![ControlQubit {
                qubit: i,
                state: ControlState::One,
            }],
            params: vec![],
        }));
    }
    QuantumProgram {
        ir_version: IR_VERSION,
        n_qubits,
        classical_registers: vec![],
        parameters: vec![],
        instructions,
        metadata: Default::default(),
    }
}

/// A non-Clifford, densely/all-to-all entangled circuit on `n_qubits`: `Rx(0)` then
/// a `CNOT(i, j)` for EVERY pair `i < j`. `estimate()`'s `EntanglingConnectivity`
/// classifies it `Dense` (most pairs are long-range in IR index order, and the
/// interior qubits each connect to 3+ others).
pub fn dense_entangled_circuit(n_qubits: u32) -> QuantumProgram {
    let mut instructions = vec![Instruction::Gate(GateInstruction {
        gate: GateKind::Rx,
        qubits: vec![0],
        controls: vec![],
        params: vec![ParamValue::Literal(0.3)],
    })];
    for i in 0..n_qubits {
        for j in (i + 1)..n_qubits {
            instructions.push(Instruction::Gate(GateInstruction {
                gate: GateKind::X,
                qubits: vec![j],
                controls: vec![ControlQubit {
                    qubit: i,
                    state: ControlState::One,
                }],
                params: vec![],
            }));
        }
    }
    QuantumProgram {
        ir_version: IR_VERSION,
        n_qubits,
        classical_registers: vec![],
        parameters: vec![],
        instructions,
        metadata: Default::default(),
    }
}

pub fn descriptor(id: &str, family: BackendFamily, caps: BackendCapabilities) -> BackendDescriptor {
    BackendDescriptor {
        id: BackendId(id.to_string()),
        family,
        capabilities: caps,
    }
}

pub fn caps(overrides: impl FnOnce(&mut BackendCapabilities)) -> BackendCapabilities {
    let mut c = BackendCapabilities {
        supports_density_matrix: false,
        supports_distributed: false,
        supports_noise: false,
        supports_gpu: false,
        supports_mps: false,
        supports_stabilizer: false,
        is_exact_capable: true,
        max_qubits_statevector: None,
        max_qubits_density_matrix: None,
        requires_hardware: false,
    };
    overrides(&mut c);
    c
}

pub fn stabilizer_descriptor(id: &str) -> BackendDescriptor {
    descriptor(
        id,
        BackendFamily::Stabilizer,
        caps(|c| {
            c.supports_stabilizer = true;
            c.is_exact_capable = true;
        }),
    )
}

pub fn sv_cpu_descriptor(id: &str) -> BackendDescriptor {
    descriptor(
        id,
        BackendFamily::StatevectorCpu,
        caps(|c| c.is_exact_capable = true),
    )
}

pub fn sv_gpu_descriptor(id: &str) -> BackendDescriptor {
    descriptor(
        id,
        BackendFamily::StatevectorGpu,
        caps(|c| {
            c.supports_gpu = true;
            c.is_exact_capable = true;
        }),
    )
}

pub fn mps_descriptor(id: &str) -> BackendDescriptor {
    descriptor(
        id,
        BackendFamily::MatrixProductState,
        caps(|c| {
            c.supports_mps = true;
            c.is_exact_capable = true;
        }),
    )
}

pub fn quest_ffi_descriptor(id: &str) -> BackendDescriptor {
    descriptor(
        id,
        BackendFamily::QuestFfi,
        caps(|c| {
            c.supports_distributed = true;
            c.supports_density_matrix = true;
            c.is_exact_capable = true;
        }),
    )
}

pub fn trajectory_descriptor(id: &str) -> BackendDescriptor {
    descriptor(
        id,
        BackendFamily::Trajectory,
        caps(|c| {
            c.supports_noise = true;
            c.is_exact_capable = false; // sampling — never exact, by construction
        }),
    )
}

pub fn hardware_descriptor(id: &str) -> BackendDescriptor {
    descriptor(
        id,
        BackendFamily::Hardware,
        caps(|c| {
            c.requires_hardware = true;
            c.supports_noise = true;
            c.is_exact_capable = false; // real hardware is never exact
        }),
    )
}
