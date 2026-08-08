//! Integration tests for [`eg_quantum_hardware::azure::AzureQuantumBackend`],
//! entirely against [`eg_quantum_hardware::transport::MockTransport`] -- no real
//! network access, no real Azure AD app registration.
#![cfg(feature = "azure")]

use eg_quantum_core::backend::{JobStatus, QuantumBackend, RunOptions};
use eg_quantum_core::ir::{
    ClassicalBitRef, ClassicalRegister, ControlQubit, ControlState, GateInstruction, GateKind,
    Instruction, QuantumProgram, IR_VERSION,
};
use eg_quantum_hardware::azure::{
    AzureQuantumBackend, AZURE_QUANTUM_BUDGET_UNITS_ENV, AZURE_QUANTUM_BUDGET_WINDOW_DAYS_ENV,
    AZURE_QUANTUM_CLIENT_ID_ENV, AZURE_QUANTUM_CLIENT_SECRET_ENV, AZURE_QUANTUM_LOCATION_ENV,
    AZURE_QUANTUM_RESOURCE_GROUP_ENV, AZURE_QUANTUM_SUBSCRIPTION_ID_ENV,
    AZURE_QUANTUM_TENANT_ID_ENV, AZURE_QUANTUM_WORKSPACE_ENV,
};
use eg_quantum_hardware::credentials::StaticCredentials;
use eg_quantum_hardware::transport::{Method, MockTransport};

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

fn full_creds(budget_units: &str) -> StaticCredentials {
    StaticCredentials::new()
        .with(AZURE_QUANTUM_TENANT_ID_ENV, "tenant")
        .with(AZURE_QUANTUM_CLIENT_ID_ENV, "client")
        .with(AZURE_QUANTUM_CLIENT_SECRET_ENV, "secret")
        .with(AZURE_QUANTUM_SUBSCRIPTION_ID_ENV, "sub")
        .with(AZURE_QUANTUM_RESOURCE_GROUP_ENV, "rg")
        .with(AZURE_QUANTUM_WORKSPACE_ENV, "ws")
        .with(AZURE_QUANTUM_LOCATION_ENV, "eastus")
        .with(AZURE_QUANTUM_BUDGET_UNITS_ENV, budget_units)
        .with(AZURE_QUANTUM_BUDGET_WINDOW_DAYS_ENV, "30")
}

const AAD_URL: &str = "https://login.microsoftonline.com/tenant/oauth2/v2.0/token";
const API_BASE: &str = "https://eastus.quantum.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Quantum/workspaces/ws";

fn queue_aad_ok(mock: &MockTransport) {
    mock.push(
        Method::Post,
        AAD_URL,
        200,
        serde_json::json!({"access_token": "tok-xyz"}),
    );
}

#[test]
fn refuses_to_submit_without_an_explicit_budget() {
    // No AZURE_QUANTUM_BUDGET_UNITS/_WINDOW_DAYS in these credentials -- unlike
    // IBM/Braket (which have a fixed free-tier quota this crate seeds by default),
    // Azure Quantum's "occasional credits" have no fixed shape, so this adapter must
    // refuse outright rather than silently allowing unlimited submissions.
    let creds = StaticCredentials::new()
        .with(AZURE_QUANTUM_TENANT_ID_ENV, "tenant")
        .with(AZURE_QUANTUM_CLIENT_ID_ENV, "client")
        .with(AZURE_QUANTUM_CLIENT_SECRET_ENV, "secret")
        .with(AZURE_QUANTUM_SUBSCRIPTION_ID_ENV, "sub")
        .with(AZURE_QUANTUM_RESOURCE_GROUP_ENV, "rg")
        .with(AZURE_QUANTUM_WORKSPACE_ENV, "ws")
        .with(AZURE_QUANTUM_LOCATION_ENV, "eastus");
    let backend = AzureQuantumBackend::with_parts(creds, MockTransport::new());
    let program = bell_circuit();

    let err = backend
        .submit(&program, &RunOptions::default())
        .expect_err("must refuse without an explicit operator-declared budget");
    match err {
        eg_quantum_core::backend::BackendError::ResourceLimit(msg) => {
            assert!(
                msg.contains(AZURE_QUANTUM_BUDGET_UNITS_ENV),
                "message was: {msg}"
            );
        }
        other => panic!("expected ResourceLimit, got {other:?}"),
    }
}

#[test]
fn submit_poll_result_happy_path_once_budget_is_declared() {
    let mock = MockTransport::new();
    queue_aad_ok(&mock);
    mock.push(
        Method::Post,
        format!("{API_BASE}/jobs"),
        201,
        serde_json::json!({"id": "job-1"}),
    );
    queue_aad_ok(&mock);
    mock.push(
        Method::Get,
        format!("{API_BASE}/jobs/job-1"),
        200,
        serde_json::json!({"status": "Succeeded"}),
    );
    queue_aad_ok(&mock);
    mock.push(
        Method::Get,
        format!("{API_BASE}/jobs/job-1/results"),
        200,
        serde_json::json!({"shots": 10, "counts": {"00": 5, "11": 5}}),
    );

    let backend = AzureQuantumBackend::with_parts(full_creds("100"), mock);
    let program = bell_circuit();
    let opts = RunOptions {
        shots: Some(10),
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

    let status = backend.quota_status().expect("budget was configured");
    assert_eq!(status.remaining.0, 90);
}

#[test]
fn quota_exhaustion_refuses_once_budget_is_declared() {
    let mock = MockTransport::new();
    let backend = AzureQuantumBackend::with_parts(full_creds("5"), mock);
    let program = bell_circuit();
    let opts = RunOptions {
        shots: Some(50),
        ..Default::default()
    };
    let err = backend
        .submit(&program, &opts)
        .expect_err("must refuse when the estimated cost exceeds the declared budget");
    match err {
        eg_quantum_core::backend::BackendError::ResourceLimit(msg) => {
            assert!(msg.contains("quota exhausted"), "message was: {msg}");
        }
        other => panic!("expected ResourceLimit, got {other:?}"),
    }
}

#[test]
fn capabilities_never_claim_exactness() {
    let backend = AzureQuantumBackend::with_parts(full_creds("100"), MockTransport::new());
    let caps = backend.capabilities();
    assert!(!caps.is_exact_capable);
    assert!(caps.requires_hardware);
}
