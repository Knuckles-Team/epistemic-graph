//! Proves the `QuantumBackend` trait shape is actually implementable end-to-end
//! (submit/poll/result/cancel/run) by a minimal in-process stub — without linking
//! any real simulator, vendor SDK, CUDA, or MPI, which is exactly the point at Q0.

mod common;

use eg_quantum_core::{
    BackendCapabilities, BackendError, BackendFamily, BackendId, JobHandle, JobStatus, Outcome,
    QuantumBackend, QuantumProgram, QuantumResult, RunOptions,
};
use std::collections::BTreeMap;
use std::sync::Mutex;

/// A trivial in-process stub: "runs" any program by immediately producing a fixed
/// exact result. Exists only to prove the trait is object-safe and end-to-end usable.
struct StubExactBackend {
    next_job: Mutex<u64>,
    jobs: Mutex<BTreeMap<u64, QuantumResult>>,
}

impl StubExactBackend {
    fn new() -> Self {
        StubExactBackend {
            next_job: Mutex::new(0),
            jobs: Mutex::new(BTreeMap::new()),
        }
    }
}

impl QuantumBackend for StubExactBackend {
    fn backend_id(&self) -> BackendId {
        BackendId("stub-exact".to_string())
    }

    fn family(&self) -> BackendFamily {
        BackendFamily::Stabilizer
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            supports_density_matrix: false,
            supports_distributed: false,
            supports_noise: false,
            supports_gpu: false,
            supports_mps: false,
            supports_stabilizer: true,
            is_exact_capable: true,
            max_qubits_statevector: None,
            max_qubits_density_matrix: None,
            requires_hardware: false,
        }
    }

    fn submit(
        &self,
        program: &QuantumProgram,
        opts: &RunOptions,
    ) -> Result<JobHandle, BackendError> {
        let circuit_hash = program
            .circuit_hash()
            .map_err(|e| BackendError::InvalidProgram(e.to_string()))?;
        let result = QuantumResult::new_exact(
            self.backend_id(),
            eg_quantum_core::Formalism::Stabilizer,
            opts.seed,
            opts.shots,
            circuit_hash,
            0,
            0,
            Outcome::Counts(BTreeMap::from([(
                "0".repeat(program.n_qubits as usize),
                opts.shots.unwrap_or(1),
            )])),
        );
        let mut next = self.next_job.lock().unwrap();
        let id = *next;
        *next += 1;
        self.jobs.lock().unwrap().insert(id, result);
        Ok(JobHandle(id))
    }

    fn poll(&self, job: JobHandle) -> Result<JobStatus, BackendError> {
        if self.jobs.lock().unwrap().contains_key(&job.0) {
            Ok(JobStatus::Completed)
        } else {
            Err(BackendError::UnknownJob)
        }
    }

    fn result(&self, job: JobHandle) -> Result<QuantumResult, BackendError> {
        self.jobs
            .lock()
            .unwrap()
            .get(&job.0)
            .cloned()
            .ok_or(BackendError::UnknownJob)
    }

    fn cancel(&self, job: JobHandle) -> Result<(), BackendError> {
        self.jobs
            .lock()
            .unwrap()
            .remove(&job.0)
            .map(|_| ())
            .ok_or(BackendError::UnknownJob)
    }

    fn run(
        &self,
        program: &QuantumProgram,
        opts: &RunOptions,
    ) -> Result<QuantumResult, BackendError> {
        let job = self.submit(program, opts)?;
        self.result(job)
    }
}

#[test]
fn stub_backend_submit_poll_result_round_trip() {
    let backend = StubExactBackend::new();
    let program = common::bell_circuit();
    let opts = RunOptions {
        shots: Some(100),
        seed: Some(7),
        ..Default::default()
    };

    let job = backend.submit(&program, &opts).expect("submit");
    assert_eq!(backend.poll(job).unwrap(), JobStatus::Completed);
    let result = backend.result(job).expect("result");
    assert!(result.is_exact());
    assert_eq!(result.backend_id, backend.backend_id());
}

#[test]
fn stub_backend_run_is_equivalent_to_submit_then_result() {
    let backend = StubExactBackend::new();
    let program = common::bell_circuit();
    let result = backend.run(&program, &RunOptions::default()).expect("run");
    assert!(result.is_exact());
    // An exact stabilizer-backend result must be usable as a hard constraint.
    assert!(result.into_hard_constraint().is_ok());
}

#[test]
fn cancel_then_poll_reports_unknown_job() {
    let backend = StubExactBackend::new();
    let program = common::bell_circuit();
    let job = backend.submit(&program, &RunOptions::default()).unwrap();
    backend.cancel(job).expect("cancel");
    assert!(matches!(backend.poll(job), Err(BackendError::UnknownJob)));
}

#[test]
fn dyn_trait_object_works() {
    // The trait must be object-safe: a heterogeneous registry of backends is the
    // whole point of the vendor-neutral design.
    let backend: Box<dyn QuantumBackend> = Box::new(StubExactBackend::new());
    let program = common::bell_circuit();
    let result = backend.run(&program, &RunOptions::default()).unwrap();
    assert!(result.is_exact());
}
