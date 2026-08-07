//! End-to-end smoke tests for lane Q1 (register D-QN-1): Bell state, GHZ state, and
//! a small non-trivial Clifford circuit through the real `QuantumBackend` trait
//! objects, plus the planner-routing proof that a Clifford circuit selects
//! `stabilizer`, never `statevector`, now that both are real backends instead of
//! Q0's stub `BackendDescriptor`s.

use eg_quantum_core::backend::{
    BackendCapabilities, BackendDescriptor, BackendFamily, QuantumBackend, RunOptions,
};
use eg_quantum_core::estimate::{estimate, EstimateOptions};
use eg_quantum_core::ir::{
    ClassicalBitRef, ClassicalRegister, ControlQubit, ControlState, GateInstruction, GateKind,
    Instruction, ParamValue, QuantumProgram,
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
        ir_version: eg_quantum_core::ir::IR_VERSION,
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
        ir_version: eg_quantum_core::ir::IR_VERSION,
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

/// A small Clifford circuit beyond Bell/GHZ: exercises S (not just H/CNOT).
/// H(0); S(0); CNOT(0,1); H(1); measure both.
fn small_clifford_program() -> QuantumProgram {
    QuantumProgram {
        ir_version: eg_quantum_core::ir::IR_VERSION,
        n_qubits: 2,
        classical_registers: vec![ClassicalRegister {
            name: "c".to_string(),
            n_bits: 2,
        }],
        parameters: vec![],
        instructions: vec![
            gate(GateKind::H, &[0]),
            gate(GateKind::S, &[0]),
            cnot(0, 1),
            gate(GateKind::H, &[1]),
            measure(0, "c", 0),
            measure(1, "c", 1),
        ],
        metadata: Default::default(),
    }
}

/// A non-Clifford circuit: an arbitrary-angle Rx rotation. Used to prove the
/// planner does NOT route a non-Clifford circuit to `stabilizer`.
fn non_clifford_program() -> QuantumProgram {
    QuantumProgram {
        ir_version: eg_quantum_core::ir::IR_VERSION,
        n_qubits: 1,
        classical_registers: vec![ClassicalRegister {
            name: "c".to_string(),
            n_bits: 1,
        }],
        parameters: vec![],
        instructions: vec![
            Instruction::Gate(GateInstruction {
                gate: GateKind::Rx,
                qubits: vec![0],
                controls: vec![],
                params: vec![ParamValue::Literal(0.6180339887)],
            }),
            measure(0, "c", 0),
        ],
        metadata: Default::default(),
    }
}

fn only_shots(seed: u64) -> RunOptions {
    RunOptions {
        shots: Some(200),
        seed: Some(seed),
        noise_model_id: None,
        parameter_bindings: Default::default(),
        timeout_ms: None,
    }
}

fn assert_only_keys(outcome: &Outcome, allowed: &[&str]) {
    match outcome {
        Outcome::Counts(counts) => {
            for key in counts.keys() {
                assert!(
                    allowed.contains(&key.as_str()),
                    "unexpected outcome key '{key}', allowed: {allowed:?}"
                );
            }
            assert!(!counts.is_empty(), "expected at least one outcome key");
        }
        other => panic!("expected Outcome::Counts, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Statevector backend: Bell, GHZ, small Clifford.
// ---------------------------------------------------------------------------

#[test]
fn statevector_bell_state_only_correlated_outcomes() {
    let sim = StateVectorSimulator::new();
    let result = sim.run(&bell_program(), &only_shots(1)).expect("run");
    assert!(result.is_exact());
    assert_only_keys(&result.outcome, &["00", "11"]);
}

#[test]
fn statevector_ghz_state_only_correlated_outcomes() {
    let sim = StateVectorSimulator::new();
    let result = sim.run(&ghz_program(), &only_shots(2)).expect("run");
    assert!(result.is_exact());
    assert_only_keys(&result.outcome, &["000", "111"]);
}

#[test]
fn statevector_small_clifford_circuit_runs_exactly() {
    let sim = StateVectorSimulator::new();
    let result = sim
        .run(&small_clifford_program(), &only_shots(3))
        .expect("run");
    assert!(result.is_exact());
    // H,S,CNOT,H on |00> is a valid Clifford circuit; every one of the 4 outcomes
    // is structurally possible (unlike Bell/GHZ's restricted correlated set) -- the
    // assertion here is just that it runs to completion, exactly, with a
    // well-formed Outcome::Counts.
    assert_only_keys(&result.outcome, &["00", "01", "10", "11"]);
}

// ---------------------------------------------------------------------------
// Stabilizer backend: Bell, GHZ, small Clifford -- same programs, same contract.
// ---------------------------------------------------------------------------

#[test]
fn stabilizer_bell_state_only_correlated_outcomes() {
    let sim = StabilizerSimulator::new();
    let result = sim.run(&bell_program(), &only_shots(10)).expect("run");
    assert!(result.is_exact());
    assert_only_keys(&result.outcome, &["00", "11"]);
}

#[test]
fn stabilizer_ghz_state_only_correlated_outcomes() {
    let sim = StabilizerSimulator::new();
    let result = sim.run(&ghz_program(), &only_shots(11)).expect("run");
    assert!(result.is_exact());
    assert_only_keys(&result.outcome, &["000", "111"]);
}

#[test]
fn stabilizer_small_clifford_circuit_runs_exactly() {
    let sim = StabilizerSimulator::new();
    let result = sim
        .run(&small_clifford_program(), &only_shots(12))
        .expect("run");
    assert!(result.is_exact());
    assert_only_keys(&result.outcome, &["00", "01", "10", "11"]);
}

#[test]
fn stabilizer_rejects_a_non_clifford_circuit() {
    let sim = StabilizerSimulator::new();
    let err = sim
        .run(&non_clifford_program(), &only_shots(13))
        .expect_err("a non-Clifford circuit must be rejected, not silently mis-simulated");
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("clifford"),
        "expected a Clifford-related rejection message, got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// ★ The planner routing proof: a Clifford circuit selects `stabilizer`, never
// `statevector`, now that both are real backends (not Q0's stub descriptors).
// ---------------------------------------------------------------------------

fn descriptor_for(backend: &dyn QuantumBackend) -> BackendDescriptor {
    BackendDescriptor {
        id: backend.backend_id(),
        family: backend.family(),
        capabilities: backend.capabilities(),
    }
}

#[test]
fn clifford_circuit_routes_to_stabilizer_never_statevector() {
    let sv = StateVectorSimulator::new();
    let stab = StabilizerSimulator::new();
    let available = vec![descriptor_for(&sv), descriptor_for(&stab)];

    let program = ghz_program();
    assert!(program.is_clifford(), "GHZ must be recognized as Clifford");
    let est = estimate(&program, &EstimateOptions::default());
    let decision = select_backend(&est, &available, &PlannerOptions::default())
        .expect("a backend must be selected");

    assert_eq!(decision.family, BackendFamily::Stabilizer);
    assert_eq!(decision.rule, PlannerRule::R1CliffordStabilizer);
    assert_eq!(decision.chosen, stab.backend_id());
}

#[test]
fn clifford_circuit_still_routes_to_stabilizer_when_statevector_is_listed_first() {
    // Registration order must not matter -- R1 is a rule over BackendFamily, not
    // "whichever descriptor happens to appear first."
    let sv = StateVectorSimulator::new();
    let stab = StabilizerSimulator::new();
    let available = vec![descriptor_for(&sv), descriptor_for(&stab)];

    let program = bell_program();
    let est = estimate(&program, &EstimateOptions::default());
    let decision = select_backend(&est, &available, &PlannerOptions::default())
        .expect("a backend must be selected");
    assert_eq!(decision.family, BackendFamily::Stabilizer);
}

#[test]
fn non_clifford_circuit_routes_to_statevector_not_stabilizer() {
    let sv = StateVectorSimulator::new();
    let stab = StabilizerSimulator::new();
    let available = vec![descriptor_for(&stab), descriptor_for(&sv)];

    let program = non_clifford_program();
    assert!(!program.is_clifford());
    let est = estimate(&program, &EstimateOptions::default());
    let decision = select_backend(&est, &available, &PlannerOptions::default())
        .expect("a backend must be selected");

    assert_eq!(decision.family, BackendFamily::StatevectorCpu);
    assert_ne!(decision.family, BackendFamily::Stabilizer);
}

#[test]
fn a_result_from_either_backend_can_become_a_hard_constraint() {
    // Cross-check against Q0's own load-bearing contract (result.rs): an exact
    // result from either of THIS lane's real backends must be usable as a
    // HardConstraint, not just structurally claim `is_exact()`.
    let sv = StateVectorSimulator::new();
    let sv_result = sv.run(&bell_program(), &only_shots(20)).expect("run");
    assert!(sv_result.into_hard_constraint().is_ok());

    let stab = StabilizerSimulator::new();
    let stab_result = stab.run(&bell_program(), &only_shots(21)).expect("run");
    assert!(stab_result.into_hard_constraint().is_ok());
}

#[test]
fn noise_request_is_rejected_by_both_backends_not_silently_ignored() {
    let mut opts = only_shots(30);
    opts.noise_model_id = Some("depolarizing".to_string());

    let sv = StateVectorSimulator::new();
    assert!(sv.run(&bell_program(), &opts).is_err());

    let stab = StabilizerSimulator::new();
    assert!(stab.run(&bell_program(), &opts).is_err());
}

#[test]
fn capabilities_advertise_this_lanes_backend_shape_correctly() {
    let sv = StateVectorSimulator::new();
    let sv_caps: BackendCapabilities = sv.capabilities();
    assert!(sv_caps.is_exact_capable);
    assert!(!sv_caps.supports_stabilizer);
    assert!(!sv_caps.supports_noise);
    assert!(sv_caps.max_qubits_statevector.is_some());

    let stab = StabilizerSimulator::new();
    let stab_caps: BackendCapabilities = stab.capabilities();
    assert!(stab_caps.is_exact_capable);
    assert!(stab_caps.supports_stabilizer);
    assert!(!stab_caps.supports_noise);
}

#[test]
fn submit_poll_result_lifecycle_works_for_statevector() {
    let sv = StateVectorSimulator::new();
    let handle = sv.submit(&bell_program(), &only_shots(40)).expect("submit");
    assert_eq!(
        sv.poll(handle).expect("poll"),
        eg_quantum_core::backend::JobStatus::Completed
    );
    assert!(sv.result(handle).expect("result").is_exact());
    sv.cancel(handle)
        .expect("cancel of a completed job is a no-op, not an error");
}

#[test]
fn submit_poll_result_lifecycle_works_for_stabilizer() {
    let stab = StabilizerSimulator::new();
    let handle = stab
        .submit(&bell_program(), &only_shots(41))
        .expect("submit");
    assert_eq!(
        stab.poll(handle).expect("poll"),
        eg_quantum_core::backend::JobStatus::Completed
    );
    assert!(stab.result(handle).expect("result").is_exact());
    stab.cancel(handle)
        .expect("cancel of a completed job is a no-op");
}
