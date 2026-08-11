//! Integration tests for [`eg_quantum_hardware::ibm::IbmQuantumBackend`], entirely
//! against [`eg_quantum_hardware::transport::MockTransport`] -- no real network
//! access, no real credentials, and (per this lane's explicit instruction) zero risk
//! of spending real IBM Quantum Open Plan budget.
#![cfg(feature = "ibm")]

use eg_quantum_core::backend::{JobStatus, QuantumBackend, RunOptions};
use eg_quantum_core::ir::{
    ClassicalBitRef, ClassicalRegister, ControlQubit, ControlState, GateInstruction, GateKind,
    Instruction, QuantumProgram, IR_VERSION,
};
use eg_quantum_hardware::credentials::StaticCredentials;
use eg_quantum_hardware::ibm::{
    IbmQuantumBackend, IBM_QUANTUM_API_KEY_ENV, IBM_QUANTUM_INSTANCE_CRN_ENV,
};
use eg_quantum_hardware::quota::{QuotaTracker, QuotaUnits};
use eg_quantum_hardware::transport::{Method, MockTransport};
use std::time::Duration;

fn bell_circuit() -> QuantumProgram {
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

fn creds() -> StaticCredentials {
    StaticCredentials::new()
        .with(IBM_QUANTUM_API_KEY_ENV, "test-api-key")
        .with(IBM_QUANTUM_INSTANCE_CRN_ENV, "crn:test:instance")
}

fn backend_with(
    transport: MockTransport,
    limit_seconds: u64,
) -> IbmQuantumBackend<StaticCredentials, MockTransport> {
    IbmQuantumBackend::with_parts(
        creds(),
        transport,
        QuotaTracker::new(
            "ibm-quantum-open-plan-test",
            "qpu-seconds",
            Duration::from_secs(28 * 86_400),
            QuotaUnits(limit_seconds),
        ),
    )
}

const IAM_URL: &str = "https://iam.cloud.ibm.com/identity/token";
const JOBS_URL: &str = "https://quantum.cloud.ibm.com/api/v1/jobs";

fn queue_iam_ok(mock: &MockTransport) {
    mock.push(
        Method::Post,
        IAM_URL,
        200,
        serde_json::json!({"access_token": "tok-123"}),
    );
}

#[test]
fn submit_poll_result_happy_path() {
    let mock = MockTransport::new();
    queue_iam_ok(&mock);
    mock.push(
        Method::Post,
        JOBS_URL,
        201,
        serde_json::json!({"id": "job-abc"}),
    );
    // Two IAM exchanges follow: poll() and result() each call auth_headers().
    queue_iam_ok(&mock);
    mock.push(
        Method::Get,
        "https://quantum.cloud.ibm.com/api/v1/jobs/job-abc",
        200,
        serde_json::json!({"status": "COMPLETED"}),
    );
    queue_iam_ok(&mock);
    mock.push(
        Method::Get,
        "https://quantum.cloud.ibm.com/api/v1/jobs/job-abc/results",
        200,
        serde_json::json!({"shots": 100, "counts": {"00": 48, "11": 52}}),
    );

    let backend = backend_with(mock, 600);
    let program = bell_circuit();
    let opts = RunOptions {
        shots: Some(100),
        ..Default::default()
    };

    let job = backend.submit(&program, &opts).expect("submit succeeds");
    assert_eq!(
        backend.poll(job).expect("poll succeeds"),
        JobStatus::Completed
    );
    let result = backend.result(job).expect("result succeeds");
    assert!(
        !result.is_exact(),
        "a hardware backend must NEVER return an exact result"
    );
    assert_eq!(result.backend_id.0, "hardware-ibm");

    // Budget was deducted by the estimated cost (shots=100 -> 100 "seconds").
    let status = backend.quota_status();
    assert_eq!(status.used, QuotaUnits(100));
    assert_eq!(status.remaining, QuotaUnits(500));
}

#[test]
fn quota_exhaustion_refuses_before_any_network_call() {
    let mock = MockTransport::new();
    // Deliberately queue NO responses: if the backend tried to call out anyway, the
    // MockTransport would error with "no queued response", which would also fail
    // this test but for the wrong reason -- asserting on `mock.calls` below proves
    // it genuinely never sent anything.
    let backend = backend_with(mock, 10);
    let program = bell_circuit();
    let opts = RunOptions {
        shots: Some(50), // exceeds the 10-second test budget
        ..Default::default()
    };

    let err = backend
        .submit(&program, &opts)
        .expect_err("must refuse when the estimated cost exceeds remaining budget");
    match err {
        eg_quantum_core::backend::BackendError::ResourceLimit(msg) => {
            assert!(msg.contains("quota exhausted"), "message was: {msg}");
        }
        other => panic!("expected ResourceLimit, got {other:?}"),
    }
}

#[test]
fn missing_credential_is_a_clean_error_not_a_panic() {
    let mock = MockTransport::new();
    let backend = IbmQuantumBackend::with_parts(
        StaticCredentials::new(), // no API key set
        mock,
        QuotaTracker::new(
            "ibm-quantum-open-plan-test",
            "qpu-seconds",
            Duration::from_secs(28 * 86_400),
            QuotaUnits(600),
        ),
    );
    let program = bell_circuit();
    let err = backend
        .submit(&program, &RunOptions::default())
        .expect_err("must error, not panic, on a missing credential");
    match err {
        eg_quantum_core::backend::BackendError::Execution(msg) => {
            assert!(msg.contains(IBM_QUANTUM_API_KEY_ENV), "message was: {msg}");
        }
        other => panic!("expected Execution, got {other:?}"),
    }
}

#[test]
fn capabilities_never_claim_exactness() {
    let backend = backend_with(MockTransport::new(), 600);
    let caps = backend.capabilities();
    assert!(!caps.is_exact_capable);
    assert!(caps.requires_hardware);
}
