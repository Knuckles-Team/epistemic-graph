//! The four PROGRAM.md acceptance checks for the quantum planner, against stub
//! backends (no simulator exists at Q0 — these are pure `BackendDescriptor` values).
//!
//! 1. A Clifford circuit must never select statevector when stabilizer is available.
//! 2. `n` over the memory bound must never silently select `sv-*`.
//! 3. A noise request must never return `exact: true`.
//! 4. An explicit `backend_id` override is honoured AND audited.

mod common;

use eg_quantum_core::{
    estimate, BackendFamily, BackendId, EntanglingConnectivity, EstimateOptions, NoiseRequest,
    PlannerError, PlannerOptions, PlannerRule, QuantumResult,
};

// ── 1. Clifford never selects statevector when stabilizer is available ───────────

#[test]
fn clifford_circuit_prefers_stabilizer_over_statevector() {
    let program = common::bell_circuit();
    let est = estimate(&program, &EstimateOptions::default());
    assert!(est.is_clifford);

    let available = vec![
        // Statevector registered FIRST, deliberately, so a naive "first registered"
        // implementation would fail this test if it existed.
        common::sv_cpu_descriptor("sv-cpu"),
        common::stabilizer_descriptor("stabilizer"),
    ];
    let decision = eg_quantum_core::select_backend(&est, &available, &PlannerOptions::default())
        .expect("must select");
    assert_eq!(decision.family, BackendFamily::Stabilizer);
    assert_eq!(decision.chosen, BackendId("stabilizer".to_string()));
    assert_eq!(decision.rule, PlannerRule::R1CliffordStabilizer);
}

#[test]
fn clifford_circuit_across_many_registered_statevector_backends_still_never_picks_one() {
    let program = common::bell_circuit();
    let est = estimate(&program, &EstimateOptions::default());

    let available = vec![
        common::sv_cpu_descriptor("sv-cpu-a"),
        common::sv_gpu_descriptor("sv-gpu-a"),
        common::sv_cpu_descriptor("sv-cpu-b"),
        common::stabilizer_descriptor("stabilizer"),
        common::quest_ffi_descriptor("quest-ffi"),
    ];
    let decision = eg_quantum_core::select_backend(&est, &available, &PlannerOptions::default())
        .expect("must select");
    assert_eq!(decision.family, BackendFamily::Stabilizer);
}

// ── 2. n over the memory bound never silently selects sv-* ───────────────────────

#[test]
fn over_memory_bound_never_selects_statevector_falls_back_to_distributed() {
    let program = common::non_clifford_circuit(20);
    // 20 qubits -> 16 MiB statevector; a 1 KiB bound makes it structurally
    // impossible for any sv-* family regardless of what is registered.
    let opts = EstimateOptions {
        memory_bound_bytes: Some(1024),
        ..Default::default()
    };
    let est = estimate(&program, &opts);
    assert!(est.forbidden.contains(&BackendFamily::StatevectorCpu));
    assert!(est.forbidden.contains(&BackendFamily::StatevectorGpu));

    let available = vec![
        common::sv_cpu_descriptor("sv-cpu"),
        common::sv_gpu_descriptor("sv-gpu"),
        common::quest_ffi_descriptor("quest-ffi"), // no fixed byte ceiling: distributed
    ];
    let decision = eg_quantum_core::select_backend(&est, &available, &PlannerOptions::default())
        .expect("must select");
    assert_ne!(decision.family, BackendFamily::StatevectorCpu);
    assert_ne!(decision.family, BackendFamily::StatevectorGpu);
    assert_eq!(decision.family, BackendFamily::QuestFfi);
}

#[test]
fn over_memory_bound_with_no_viable_fallback_errors_rather_than_silently_picking_sv() {
    let program = common::non_clifford_circuit(20);
    let opts = EstimateOptions {
        memory_bound_bytes: Some(1024),
        ..Default::default()
    };
    let est = estimate(&program, &opts);

    // ONLY sv-* registered — nothing else can take this circuit under the bound.
    let available = vec![
        common::sv_cpu_descriptor("sv-cpu"),
        common::sv_gpu_descriptor("sv-gpu"),
    ];
    let result = eg_quantum_core::select_backend(&est, &available, &PlannerOptions::default());
    assert!(
        matches!(result, Err(PlannerError::NoBackendAvailable { .. })),
        "must fail closed, not silently select a forbidden sv-* family: {result:?}"
    );
}

#[test]
fn per_descriptor_qubit_ceiling_is_also_respected_even_under_a_generous_byte_bound() {
    // A specific sv-cpu instance can cap lower than the raw byte-memory bound would
    // allow; the planner must respect that per-descriptor ceiling too.
    let program = common::non_clifford_circuit(10);
    let est = estimate(&program, &EstimateOptions::default()); // no global byte bound
    assert!(est.forbidden.is_empty());

    let mut capped = common::sv_cpu_descriptor("sv-cpu-capped");
    capped.capabilities.max_qubits_statevector = Some(5); // circuit needs 10
    let available = vec![capped];
    let result = eg_quantum_core::select_backend(&est, &available, &PlannerOptions::default());
    assert!(matches!(
        result,
        Err(PlannerError::NoBackendAvailable { .. })
    ));
}

// ── 3. A noise request never returns exact: true ─────────────────────────────────

#[test]
fn noise_request_routes_to_a_non_exact_capable_family() {
    let program = common::non_clifford_circuit(4);
    let opts = EstimateOptions {
        noise: Some(NoiseRequest::declared("depolarizing-p0.01")),
        shots: Some(1000), // sampling, not exact density matrix
        ..Default::default()
    };
    let est = estimate(&program, &opts);
    assert!(est.has_non_clifford_noise);
    assert!(!est.requires_density_matrix);

    let available = vec![
        common::stabilizer_descriptor("stabilizer"), // exact-capable; must NOT be reachable via R1 (has_non_clifford_noise)
        common::sv_cpu_descriptor("sv-cpu"),
        common::trajectory_descriptor("trajectory"),
    ];
    let decision = eg_quantum_core::select_backend(&est, &available, &PlannerOptions::default())
        .expect("must select");
    assert_eq!(decision.family, BackendFamily::Trajectory);

    let chosen_descriptor = available.iter().find(|d| d.id == decision.chosen).unwrap();
    assert!(
        !chosen_descriptor.capabilities.is_exact_capable,
        "a noise request must never route to an exact-capable-only family"
    );
}

#[test]
fn noise_request_never_yields_a_result_that_can_become_a_hard_constraint() {
    // Directly exercise the type-level guarantee: whatever backend actually ran a
    // noisy/sampled request, the RESULT it hands back must be constructed via
    // `new_inexact`, and `into_hard_constraint()` on that value is REQUIRED to fail —
    // there is no way to obtain a `HardConstraint` from it, by construction.
    let circuit_hash = common::non_clifford_circuit(2).circuit_hash().unwrap();
    let noisy = QuantumResult::new_inexact(
        BackendId("trajectory".to_string()),
        eg_quantum_core::Formalism::Trajectory,
        Some(42),
        Some(1000),
        circuit_hash,
        Some("depolarizing-p0.01".to_string()),
        Some(0.97),
        12,
        4096,
        eg_quantum_core::Outcome::Counts(std::collections::BTreeMap::from([
            ("00".to_string(), 500),
            ("11".to_string(), 500),
        ])),
    );
    assert!(!noisy.is_exact());
    let err = noisy
        .into_hard_constraint()
        .expect_err("must never succeed for a noisy/sampled result");
    assert_eq!(err.backend_id, BackendId("trajectory".to_string()));
}

#[test]
fn exact_result_can_become_a_hard_constraint() {
    let circuit_hash = common::bell_circuit().circuit_hash().unwrap();
    let exact = QuantumResult::new_exact(
        BackendId("stabilizer".to_string()),
        eg_quantum_core::Formalism::Stabilizer,
        None,
        None,
        circuit_hash,
        1,
        256,
        eg_quantum_core::Outcome::ExpectationValue {
            value: 1.0,
            stderr: None,
        },
    );
    assert!(exact.is_exact());
    let hard = exact
        .into_hard_constraint()
        .expect("exact results must be usable as hard constraints");
    assert!(hard.result().is_exact());
}

// ── 4. An explicit backend_id override is honoured AND audited ───────────────────

#[test]
fn explicit_override_is_honoured_even_against_the_clifford_fast_path() {
    let program = common::bell_circuit();
    let est = estimate(&program, &EstimateOptions::default());
    let available = vec![
        common::stabilizer_descriptor("stabilizer"),
        common::sv_cpu_descriptor("sv-cpu"),
    ];

    let opts = PlannerOptions {
        want_hardware: false,
        backend_id_override: Some(BackendId("sv-cpu".to_string())),
    };
    let decision = eg_quantum_core::select_backend(&est, &available, &opts)
        .expect("override must be honoured");

    assert_eq!(decision.chosen, BackendId("sv-cpu".to_string()));
    assert_eq!(decision.family, BackendFamily::StatevectorCpu);
    assert_eq!(decision.rule, PlannerRule::R5Override);

    // Audited: the trail must record that R0-R4 would have picked something else.
    let overrode_note = decision
        .audit
        .iter()
        .find(|e| e.rule == PlannerRule::R5Override)
        .expect("must have an R5 audit entry");
    assert!(
        overrode_note.note.contains("sv-cpu") && overrode_note.note.contains("stabilizer"),
        "audit note must name both the override target and what it overrode: {}",
        overrode_note.note
    );
}

#[test]
fn explicit_override_is_honoured_even_when_it_conflicts_with_a_hard_constraint() {
    // Override to a backend that R0-R4 could not have reached at all (over the
    // memory bound) — still honoured, still audited with a warning.
    let program = common::non_clifford_circuit(20);
    let opts = EstimateOptions {
        memory_bound_bytes: Some(1024),
        ..Default::default()
    };
    let est = estimate(&program, &opts);
    let available = vec![common::sv_cpu_descriptor("sv-cpu")];

    let planner_opts = PlannerOptions {
        want_hardware: false,
        backend_id_override: Some(BackendId("sv-cpu".to_string())),
    };
    let decision = eg_quantum_core::select_backend(&est, &available, &planner_opts)
        .expect("override must still be honoured");
    assert_eq!(decision.chosen, BackendId("sv-cpu".to_string()));
    let note = decision
        .audit
        .iter()
        .find(|e| e.rule == PlannerRule::R5Override)
        .unwrap();
    assert!(
        note.note.contains("WARNING"),
        "must flag the conflict in the audit trail: {}",
        note.note
    );
}

// ── R0: explicit hardware request restricts to the Hardware family ───────────────

#[test]
fn want_hardware_restricts_to_hardware_family_even_for_a_clifford_circuit() {
    let program = common::bell_circuit();
    let est = estimate(&program, &EstimateOptions::default());
    let available = vec![
        common::stabilizer_descriptor("stabilizer"),
        common::hardware_descriptor("qpu-1"),
    ];
    let opts = PlannerOptions {
        want_hardware: true,
        backend_id_override: None,
    };
    let decision = eg_quantum_core::select_backend(&est, &available, &opts)
        .expect("must select the hardware backend");
    assert_eq!(decision.family, BackendFamily::Hardware);
    assert_eq!(decision.rule, PlannerRule::R0HardConstraint);
}

// ── R3 (register D-QN-4): MatrixProductState preferred over statevector for
// genuinely low structural entanglement; never preferred for dense/all-to-all ─────

#[test]
fn low_entanglement_chain_circuit_prefers_mps_over_statevector() {
    let program = common::chain_entangled_circuit(4);
    let est = estimate(&program, &EstimateOptions::default());
    assert!(
        !est.is_clifford,
        "Rx breaks Cliffordness so R1 does not preempt R3"
    );
    assert_eq!(
        est.entangling_connectivity,
        EntanglingConnectivity::NearestNeighborChain
    );

    let available = vec![
        // Statevector registered FIRST, deliberately, so a naive "first registered"
        // implementation would fail this test if it existed.
        common::sv_cpu_descriptor("sv-cpu"),
        common::mps_descriptor("mps"),
    ];
    let decision = eg_quantum_core::select_backend(&est, &available, &PlannerOptions::default())
        .expect("must select");
    assert_eq!(decision.family, BackendFamily::MatrixProductState);
    assert_eq!(decision.chosen, BackendId("mps".to_string()));
    assert_eq!(decision.rule, PlannerRule::R3Structure);
}

#[test]
fn dense_entanglement_circuit_never_selects_mps_falls_back_to_statevector() {
    let program = common::dense_entangled_circuit(4);
    let est = estimate(&program, &EstimateOptions::default());
    assert!(!est.is_clifford);
    assert_eq!(est.entangling_connectivity, EntanglingConnectivity::Dense);
    assert!(
        !est.preferred.contains(&BackendFamily::MatrixProductState),
        "a densely-entangled circuit must not offer MPS as a preferred candidate at all: {:?}",
        est.preferred
    );

    let available = vec![
        common::mps_descriptor("mps"),
        common::sv_cpu_descriptor("sv-cpu"),
    ];
    let decision = eg_quantum_core::select_backend(&est, &available, &PlannerOptions::default())
        .expect("must select");
    assert_ne!(decision.family, BackendFamily::MatrixProductState);
    assert_eq!(decision.family, BackendFamily::StatevectorCpu);
    assert_eq!(decision.rule, PlannerRule::R4Placement);
}

#[test]
fn low_entanglement_circuit_selects_mps_when_it_is_the_only_registered_family() {
    // Proves the RANKING (MPS is genuinely present in Estimate::preferred for a
    // low-entanglement circuit), not merely a tie-break against statevector: no
    // statevector descriptor is registered at all here, so selection only succeeds
    // if `estimate()` actually put MatrixProductState in `preferred`.
    let program = common::chain_entangled_circuit(3);
    let est = estimate(&program, &EstimateOptions::default());
    assert_eq!(
        est.entangling_connectivity,
        EntanglingConnectivity::NearestNeighborChain
    );

    let available = vec![common::mps_descriptor("mps")];
    let decision = eg_quantum_core::select_backend(&est, &available, &PlannerOptions::default())
        .expect("must select");
    assert_eq!(decision.family, BackendFamily::MatrixProductState);
    assert_eq!(decision.rule, PlannerRule::R3Structure);
}

#[test]
fn override_to_an_unregistered_backend_is_rejected_not_silently_substituted() {
    let program = common::bell_circuit();
    let est = estimate(&program, &EstimateOptions::default());
    let available = vec![common::stabilizer_descriptor("stabilizer")];
    let opts = PlannerOptions {
        want_hardware: false,
        backend_id_override: Some(BackendId("does-not-exist".to_string())),
    };
    let result = eg_quantum_core::select_backend(&est, &available, &opts);
    assert_eq!(
        result,
        Err(PlannerError::UnknownOverrideBackend(BackendId(
            "does-not-exist".to_string()
        )))
    );
}
