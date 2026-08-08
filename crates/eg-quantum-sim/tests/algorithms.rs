//! CONCEPT:EG-KG.compute.quantum-algorithms-catalog -- well-known quantum
//! algorithms (Deutsch-Jozsa, Bernstein-Vazirani, Grover 2-qubit search) run
//! end-to-end through the REAL IR -> planner -> backend -> `QuantumResult`
//! pipeline (register `D-QN-1`), demonstrating this is a usable quantum-algorithm
//! substrate, not just a backend smoke-test shim.
//!
//! Every algorithm here is deliberately Clifford-only (H/X/Z/CNOT/CZ) so the same
//! acceptance bar `tests/smoke.rs` already established applies: the planner must
//! route to `stabilizer` (R1), the result must be `exact`, and it must be usable as
//! a `HardConstraint`. A genuinely non-Clifford algorithm (e.g. QAOA with
//! arbitrary-angle rotations, needing expectation values rather than a bitstring
//! outcome) needs the numeric bridge (lane Q6, not yet built) to be useful and is
//! deliberately left to that lane -- these three are chosen because they are
//! textbook-standard, fully expressible with what already exists (Q0's IR, Q1/Q4's
//! two backends, Q0's planner), and their correctness criterion is a deterministic
//! measurement outcome, not shot-count statistics.

use std::collections::BTreeMap;

use eg_quantum_core::backend::{BackendDescriptor, QuantumBackend, RunOptions};
use eg_quantum_core::estimate::{estimate, EstimateOptions};
use eg_quantum_core::ir::{
    ClassicalBitRef, ClassicalRegister, ControlQubit, ControlState, GateInstruction, GateKind,
    Instruction, QuantumProgram,
};
use eg_quantum_core::planner::{select_backend, PlannerOptions, PlannerRule};
use eg_quantum_core::result::Outcome;
use eg_quantum_sim::stabilizer::StabilizerSimulator;
use eg_quantum_sim::statevector::StateVectorSimulator;

fn gate(kind: GateKind, qubits: &[u32]) -> Instruction {
    Instruction::Gate(GateInstruction {
        gate: kind,
        qubits: qubits.to_vec(),
        controls: vec![],
        params: vec![],
    })
}

fn controlled(kind: GateKind, control: u32, target: u32) -> Instruction {
    Instruction::Gate(GateInstruction {
        gate: kind,
        qubits: vec![target],
        controls: vec![ControlQubit {
            qubit: control,
            state: ControlState::One,
        }],
        params: vec![],
    })
}

fn cnot(control: u32, target: u32) -> Instruction {
    controlled(GateKind::X, control, target)
}

fn cz(control: u32, target: u32) -> Instruction {
    controlled(GateKind::Z, control, target)
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

fn program(n_qubits: u32, n_cbits: u32, instructions: Vec<Instruction>) -> QuantumProgram {
    QuantumProgram {
        ir_version: eg_quantum_core::ir::IR_VERSION,
        n_qubits,
        classical_registers: vec![ClassicalRegister {
            name: "c".to_string(),
            n_bits: n_cbits,
        }],
        parameters: vec![],
        instructions,
        metadata: Default::default(),
    }
}

fn descriptor_for(backend: &dyn QuantumBackend) -> BackendDescriptor {
    BackendDescriptor {
        id: backend.backend_id(),
        family: backend.family(),
        capabilities: backend.capabilities(),
    }
}

/// Route `prog` through the REAL planner against both live backends, run it on
/// whichever it selects, and return the planner's rule plus the measured
/// `Outcome::Counts` map -- the same pipeline `tests/smoke.rs` exercises, reused
/// here so an algorithm test proves the full IR -> planner -> backend ->
/// `QuantumResult` path, not a backend called directly in isolation.
fn run_via_planner(
    prog: &QuantumProgram,
    seed: u64,
    shots: u64,
) -> (PlannerRule, BTreeMap<String, u64>) {
    let sv = StateVectorSimulator::new();
    let stab = StabilizerSimulator::new();
    let available = vec![descriptor_for(&sv), descriptor_for(&stab)];
    let est = estimate(prog, &EstimateOptions::default());
    let decision = select_backend(&est, &available, &PlannerOptions::default())
        .expect("planner must pick a backend for a valid Clifford circuit");

    let opts = RunOptions {
        shots: Some(shots),
        seed: Some(seed),
        noise_model_id: None,
        parameter_bindings: Default::default(),
        timeout_ms: None,
    };
    let result = if decision.chosen == stab.backend_id() {
        stab.run(prog, &opts).expect("stabilizer run")
    } else {
        sv.run(prog, &opts).expect("statevector run")
    };
    assert!(
        result.is_exact(),
        "every algorithm in this file is Clifford and must run exactly"
    );
    result
        .clone()
        .into_hard_constraint()
        .expect("an exact result must be usable as a HardConstraint (Q0's own contract)");
    let counts = match result.outcome {
        Outcome::Counts(c) => c,
        other => panic!("expected Outcome::Counts, got {other:?}"),
    };
    (decision.rule, counts)
}

// ---------------------------------------------------------------------------
// Deutsch-Jozsa (register D-QN-1): decide constant vs. balanced in ONE query.
// n=2 input qubits (0,1) + 1 ancilla (2); only the input register is measured.
// ---------------------------------------------------------------------------

fn deutsch_jozsa_program(oracle: Vec<Instruction>) -> QuantumProgram {
    let mut instrs = vec![
        gate(GateKind::X, &[2]), // ancilla -> |1>
        gate(GateKind::H, &[0]),
        gate(GateKind::H, &[1]),
        gate(GateKind::H, &[2]), // ancilla -> |->
    ];
    instrs.extend(oracle);
    instrs.push(gate(GateKind::H, &[0]));
    instrs.push(gate(GateKind::H, &[1]));
    instrs.push(measure(0, "c", 0));
    instrs.push(measure(1, "c", 1));
    program(3, 2, instrs)
}

#[test]
fn deutsch_jozsa_constant_zero_oracle_measures_all_zero() {
    // f(x) = 0 for all x: the oracle is the identity -- no gates at all.
    let prog = deutsch_jozsa_program(vec![]);
    let (rule, counts) = run_via_planner(&prog, 100, 64);
    assert_eq!(rule, PlannerRule::R1CliffordStabilizer);
    assert_eq!(
        counts.keys().collect::<Vec<_>>(),
        vec!["00"],
        "a constant oracle must measure all-zero on every shot, got {counts:?}"
    );
}

#[test]
fn deutsch_jozsa_constant_one_oracle_measures_all_zero() {
    // f(x) = 1 for all x: an unconditional X on the ancilla (global phase only).
    let prog = deutsch_jozsa_program(vec![gate(GateKind::X, &[2])]);
    let (rule, counts) = run_via_planner(&prog, 101, 64);
    assert_eq!(rule, PlannerRule::R1CliffordStabilizer);
    assert_eq!(
        counts.keys().collect::<Vec<_>>(),
        vec!["00"],
        "a constant oracle must measure all-zero on every shot, got {counts:?}"
    );
}

#[test]
fn deutsch_jozsa_balanced_oracle_never_measures_all_zero() {
    // f(x) = x0 (balanced: exactly half of the 2-bit inputs map to 0, half to 1),
    // implemented as the standard phase-kickback CNOT(input -> ancilla).
    let prog = deutsch_jozsa_program(vec![cnot(0, 2)]);
    let (rule, counts) = run_via_planner(&prog, 102, 64);
    assert_eq!(rule, PlannerRule::R1CliffordStabilizer);
    assert!(
        !counts.contains_key("00"),
        "a balanced oracle must never measure all-zero, got {counts:?}"
    );
}

// ---------------------------------------------------------------------------
// Bernstein-Vazirani (register D-QN-1): recover a hidden n-bit string in ONE query.
// ---------------------------------------------------------------------------

fn bernstein_vazirani_program(hidden: &[bool]) -> QuantumProgram {
    let n = hidden.len() as u32;
    let ancilla = n;
    let mut instrs = vec![gate(GateKind::X, &[ancilla])];
    for q in 0..=n {
        instrs.push(gate(GateKind::H, &[q]));
    }
    for (i, &bit) in hidden.iter().enumerate() {
        if bit {
            instrs.push(cnot(i as u32, ancilla));
        }
    }
    for q in 0..n {
        instrs.push(gate(GateKind::H, &[q]));
    }
    for q in 0..n {
        instrs.push(measure(q, "c", q));
    }
    program(n + 1, n, instrs)
}

fn bits_to_string(bits: &[bool]) -> String {
    bits.iter().map(|&b| if b { '1' } else { '0' }).collect()
}

#[test]
fn bernstein_vazirani_recovers_hidden_string_101() {
    let hidden = [true, false, true];
    let prog = bernstein_vazirani_program(&hidden);
    let (rule, counts) = run_via_planner(&prog, 200, 32);
    assert_eq!(rule, PlannerRule::R1CliffordStabilizer);
    let expected = bits_to_string(&hidden);
    assert_eq!(
        counts.keys().collect::<Vec<_>>(),
        vec![expected.as_str()],
        "expected the hidden string {expected} recovered deterministically, got {counts:?}"
    );
}

#[test]
fn bernstein_vazirani_recovers_hidden_string_all_ones() {
    let hidden = [true, true, true, true];
    let prog = bernstein_vazirani_program(&hidden);
    let (rule, counts) = run_via_planner(&prog, 201, 32);
    assert_eq!(rule, PlannerRule::R1CliffordStabilizer);
    let expected = bits_to_string(&hidden);
    assert_eq!(counts.keys().collect::<Vec<_>>(), vec![expected.as_str()]);
}

#[test]
fn bernstein_vazirani_recovers_hidden_string_all_zero() {
    let hidden = [false, false];
    let prog = bernstein_vazirani_program(&hidden);
    let (rule, counts) = run_via_planner(&prog, 202, 32);
    assert_eq!(rule, PlannerRule::R1CliffordStabilizer);
    let expected = bits_to_string(&hidden);
    assert_eq!(counts.keys().collect::<Vec<_>>(), vec![expected.as_str()]);
}

// ---------------------------------------------------------------------------
// Grover's search (register D-QN-1): 2-qubit search, ONE iteration, deterministic
// success -- N=4, M=1 gives success probability sin^2(3*theta)=1 exactly, where
// theta = asin(sqrt(1/4)) = pi/6 (the textbook N=4 special case).
// ---------------------------------------------------------------------------

fn grover_two_qubit_program() -> QuantumProgram {
    let instrs = vec![
        // Uniform superposition.
        gate(GateKind::H, &[0]),
        gate(GateKind::H, &[1]),
        // Oracle: phase-flip the marked state |11>.
        cz(0, 1),
        // Diffusion operator: 2|s><s| - I about the uniform state |s>.
        gate(GateKind::H, &[0]),
        gate(GateKind::H, &[1]),
        gate(GateKind::X, &[0]),
        gate(GateKind::X, &[1]),
        cz(0, 1),
        gate(GateKind::X, &[0]),
        gate(GateKind::X, &[1]),
        gate(GateKind::H, &[0]),
        gate(GateKind::H, &[1]),
        measure(0, "c", 0),
        measure(1, "c", 1),
    ];
    program(2, 2, instrs)
}

#[test]
fn grover_two_qubit_search_finds_marked_state_deterministically() {
    let prog = grover_two_qubit_program();
    let (rule, counts) = run_via_planner(&prog, 300, 128);
    assert_eq!(rule, PlannerRule::R1CliffordStabilizer);
    assert_eq!(
        counts.keys().collect::<Vec<_>>(),
        vec!["11"],
        "N=4/M=1 Grover with one iteration must find the marked state with \
         probability 1, got {counts:?}"
    );
}
