//! Integration tests for [`eg_quantum_hardware::braket::BraketBackend`], entirely
//! against [`eg_quantum_hardware::transport::MockTransport`] -- no real network
//! access, no real AWS credentials, no real SigV4-signed call ever leaves the
//! process.
#![cfg(feature = "braket")]

use eg_quantum_core::backend::{JobStatus, QuantumBackend, RunOptions};
use eg_quantum_core::ir::{
    ClassicalBitRef, ClassicalRegister, ControlQubit, ControlState, GateInstruction, GateKind,
    Instruction, QuantumProgram, IR_VERSION,
};
use eg_quantum_hardware::braket::{
    BraketBackend, AWS_BRAKET_ACCESS_KEY_ID_ENV, AWS_BRAKET_SECRET_ACCESS_KEY_ENV,
};
use eg_quantum_hardware::credentials::StaticCredentials;
use eg_quantum_hardware::quota::{QuotaTracker, QuotaUnits};
use eg_quantum_hardware::transport::{Method, MockTransport};
use std::sync::Arc;
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
        .with(AWS_BRAKET_ACCESS_KEY_ID_ENV, "AKIDEXAMPLE")
        .with(AWS_BRAKET_SECRET_ACCESS_KEY_ENV, "secret")
}

fn backend_with(
    transport: MockTransport,
    limit_seconds: u64,
) -> BraketBackend<StaticCredentials, MockTransport> {
    BraketBackend::with_parts(
        creds(),
        transport,
        QuotaTracker::new(
            "aws-braket-free-tier-test",
            "simulator-seconds",
            Duration::from_secs(30 * 86_400),
            QuotaUnits(limit_seconds),
        ),
    )
}

fn backend_with_shared(
    transport: Arc<MockTransport>,
    limit_seconds: u64,
) -> BraketBackend<StaticCredentials, Arc<MockTransport>> {
    BraketBackend::with_parts(
        creds(),
        transport,
        QuotaTracker::new(
            "aws-braket-free-tier-test",
            "simulator-seconds",
            Duration::from_secs(30 * 86_400),
            QuotaUnits(limit_seconds),
        ),
    )
}

const HOST: &str = "https://braket.us-east-1.amazonaws.com";

#[test]
fn submit_poll_result_happy_path() {
    let mock = MockTransport::new();
    mock.push(
        Method::Post,
        format!("{HOST}/quantum-task"),
        200,
        serde_json::json!({"quantumTaskArn": "arn:aws:braket:task/abc"}),
    );
    mock.push(
        Method::Get,
        format!("{HOST}/quantum-task/arn:aws:braket:task/abc"),
        200,
        serde_json::json!({"status": "COMPLETED"}),
    );
    mock.push(
        Method::Get,
        format!("{HOST}/quantum-task/arn:aws:braket:task/abc/result"),
        200,
        serde_json::json!({"shots": 100, "measurementCounts": {"00": 49, "11": 51}}),
    );

    let backend = backend_with(mock, 3_600);
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
    assert_eq!(result.backend_id.0, "hardware-braket");
}

#[test]
fn quota_exhaustion_refuses_before_any_network_call() {
    let mock = MockTransport::new();
    let backend = backend_with(mock, 5);
    let program = bell_circuit();
    let opts = RunOptions {
        shots: Some(3_600), // far exceeds the 5-second test budget
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
fn every_request_carries_a_sigv4_authorization_header() {
    let mock = Arc::new(MockTransport::new());
    mock.push(
        Method::Post,
        format!("{HOST}/quantum-task"),
        200,
        serde_json::json!({"quantumTaskArn": "arn:aws:braket:task/abc"}),
    );
    let backend = backend_with_shared(mock.clone(), 3_600);
    let program = bell_circuit();
    backend
        .submit(&program, &RunOptions::default())
        .expect("submit succeeds");

    let calls = mock.calls.lock().expect("mock calls mutex poisoned");
    assert_eq!(calls.len(), 1);
    let sent = &calls[0];
    let auth = sent
        .headers
        .iter()
        .find(|(k, _)| k == "Authorization")
        .map(|(_, v)| v.as_str())
        .expect("Authorization header must be present");
    assert!(auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/"));
    assert!(sent.headers.iter().any(|(k, _)| k == "X-Amz-Date"));
    assert!(sent
        .headers
        .iter()
        .any(|(k, _)| k == "X-Amz-Content-Sha256"));
}

#[test]
fn capabilities_never_claim_exactness() {
    let backend = backend_with(MockTransport::new(), 3_600);
    let caps = backend.capabilities();
    assert!(!caps.is_exact_capable);
    assert!(caps.requires_hardware);
}
