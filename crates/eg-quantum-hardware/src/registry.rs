//! Shared provider job lifecycle for the hardware adapters.
//!
//! The provider modules own credentials, URLs, request signing, and wire-status
//! translation. This module owns only the provider-independent state machine
//! boundary: one local job value, one handle registry, program preparation, and
//! the synchronous polling policy shared by IBM, Braket, and Azure.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::error::HardwareError;
use crate::quota::QuotaStatus;
use eg_quantum_core::backend::{BackendError, BackendId, JobHandle, JobStatus, RunOptions};
use eg_quantum_core::hash::CircuitHash;
use eg_quantum_core::ir::QuantumProgram;
use eg_quantum_core::result::QuantumResult;

/// The provider-independent state of one submitted hardware job.
///
/// IBM and Azure call the remote identifier a job id while Braket calls it a task
/// ARN, but all three adapters have the same local lifecycle: a remote identity,
/// terminal status, optional sampled result, quota snapshot, and the hash of the
/// locally validated program. Keeping that state in one value object leaves the
/// provider modules responsible only for translating their wire protocols.
pub(crate) struct HardwareJob {
    pub(crate) remote_id: String,
    pub(crate) status: JobStatus,
    pub(crate) result: Option<QuantumResult>,
    pub(crate) quota_at_submit: QuotaStatus,
    pub(crate) circuit_hash: CircuitHash,
}

impl HardwareJob {
    pub(crate) fn queued(
        remote_id: String,
        quota_at_submit: QuotaStatus,
        circuit_hash: CircuitHash,
    ) -> Self {
        HardwareJob {
            remote_id,
            status: JobStatus::Queued,
            result: None,
            quota_at_submit,
            circuit_hash,
        }
    }

    pub(crate) fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            JobStatus::Completed | JobStatus::Failed(_) | JobStatus::Cancelled
        )
    }

    pub(crate) fn cached_result(&self) -> Option<QuantumResult> {
        self.result.clone()
    }

    pub(crate) fn set_result(&mut self, result: QuantumResult) -> QuantumResult {
        self.result = Some(result.clone());
        result
    }

    pub(crate) fn resolve_result<F>(&mut self, fetch: F) -> Result<QuantumResult, HardwareError>
    where
        F: FnOnce(&HardwareJob) -> Result<QuantumResult, HardwareError>,
    {
        if let Some(result) = self.cached_result() {
            return Ok(result);
        }
        let result = fetch(self)?;
        Ok(self.set_result(result))
    }

    pub(crate) fn refresh_with<F>(&mut self, fetch: F) -> Result<(), HardwareError>
    where
        F: FnOnce(&HardwareJob) -> Result<JobStatus, HardwareError>,
    {
        if self.is_terminal() {
            return Ok(());
        }
        self.status = fetch(self)?;
        Ok(())
    }
}

/// Shared in-memory job registry used by each provider adapter.
///
/// The registry owns handle allocation and the mutex around mutable remote-job
/// state. It is deliberately not a provider client: authentication, endpoint
/// construction, quota policy, and status/result translation remain with each
/// provider module.
pub(crate) struct HardwareJobRegistry {
    records: Mutex<HashMap<u64, HardwareJob>>,
    next_handle: AtomicU64,
}

impl Default for HardwareJobRegistry {
    fn default() -> Self {
        HardwareJobRegistry {
            records: Mutex::new(HashMap::new()),
            next_handle: AtomicU64::new(0),
        }
    }
}

impl HardwareJobRegistry {
    pub(crate) fn insert(&self, record: HardwareJob) -> JobHandle {
        let handle = self.next_handle.fetch_add(1, Ordering::SeqCst);
        self.records
            .lock()
            .expect("job registry mutex poisoned")
            .insert(handle, record);
        JobHandle(handle)
    }

    pub(crate) fn with_record<T>(
        &self,
        job: JobHandle,
        action: impl FnOnce(&mut HardwareJob) -> Result<T, BackendError>,
    ) -> Result<T, BackendError> {
        let mut records = self.records.lock().expect("job registry mutex poisoned");
        let record = records.get_mut(&job.0).ok_or(BackendError::UnknownJob)?;
        action(record)
    }

    pub(crate) fn quota_status_at_submit(&self, job: JobHandle) -> Option<QuotaStatus> {
        self.records
            .lock()
            .expect("job registry mutex poisoned")
            .get(&job.0)
            .map(|record| record.quota_at_submit)
    }
}

/// Validate and identify a program once at the provider boundary.
///
/// The adapters all need these two operations before they can build a provider
/// envelope. This value object carries both results so the validation invariant is
/// visible in request-building code and is not copied into three clients.
pub(crate) struct PreparedCircuit {
    pub(crate) hash: CircuitHash,
    pub(crate) n_qubits: u32,
}

impl PreparedCircuit {
    pub(crate) fn from_program(program: &QuantumProgram) -> Result<Self, HardwareError> {
        program
            .validate()
            .map_err(|error| HardwareError::InvalidProgram(error.to_string()))?;
        let hash = program
            .circuit_hash()
            .map_err(|error| HardwareError::InvalidProgram(error.to_string()))?;
        Ok(PreparedCircuit {
            hash,
            n_qubits: program.n_qubits,
        })
    }
}

/// Conservative shot-based reservation shared by providers whose exact runtime
/// cost is unavailable before submission.
pub(crate) fn hardware_result(
    backend_id: &BackendId,
    record: &HardwareJob,
    body: &serde_json::Value,
    counts_field: &str,
    fidelity_field: Option<&str>,
) -> QuantumResult {
    let fidelity = fidelity_field
        .and_then(|field| body.get(field))
        .and_then(serde_json::Value::as_f64);
    QuantumResult::new_hardware_counts(
        backend_id.clone(),
        body.get("shots").and_then(serde_json::Value::as_u64),
        record.circuit_hash,
        fidelity,
        crate::transport::object_counts(body, counts_field),
    )
}

pub(crate) fn remote_id(
    body: &serde_json::Value,
    field: &str,
    missing_message: &str,
) -> Result<String, HardwareError> {
    body.get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| HardwareError::UnexpectedResponse(missing_message.to_string(), None))
}

pub(crate) fn queued_job(
    remote_id: String,
    quota_at_submit: QuotaStatus,
    circuit_hash: CircuitHash,
) -> HardwareJob {
    HardwareJob::queued(remote_id, quota_at_submit, circuit_hash)
}

/// Run one submitted hardware job to completion with bounded exponential backoff.
///
/// This is the common synchronous convenience lifecycle. The provider adapters
/// still expose submit/poll/result/cancel independently; this helper only removes
/// identical polling policy from their trait implementations.
pub(crate) fn run_to_completion<Submit, Poll, Fetch, Cancel>(
    opts: &RunOptions,
    submit: Submit,
    mut poll: Poll,
    fetch: Fetch,
    cancel: Cancel,
    timeout_message: &str,
) -> Result<QuantumResult, BackendError>
where
    Submit: FnOnce() -> Result<JobHandle, BackendError>,
    Poll: FnMut(JobHandle) -> Result<JobStatus, BackendError>,
    Fetch: FnOnce(JobHandle) -> Result<QuantumResult, BackendError>,
    Cancel: FnOnce(JobHandle) -> Result<(), BackendError>,
{
    let job = submit()?;
    let deadline = Instant::now() + Duration::from_millis(opts.timeout_ms.unwrap_or(300_000));
    let mut backoff = Duration::from_millis(500);
    loop {
        match poll(job)? {
            JobStatus::Completed => return fetch(job),
            JobStatus::Failed(message) => return Err(BackendError::Execution(message)),
            JobStatus::Cancelled => return Err(BackendError::Cancelled),
            JobStatus::Queued | JobStatus::Running => {
                if Instant::now() >= deadline {
                    let _ = cancel(job);
                    return Err(BackendError::ResourceLimit(timeout_message.to_string()));
                }
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
        }
    }
}

/// Implement the provider-neutral job lifecycle for a hardware adapter.
///
/// The provider modules supply the wire-specific `execute_submit`,
/// `refresh_status`, `fetch_result`, and `cancel_job` methods. This macro owns the
/// trait boundary shared by all adapters, including handle allocation, terminal
/// state checks, error mapping, and bounded `run()` polling.
macro_rules! impl_hardware_backend {
    (
        $backend:ident,
        supports_density_matrix = $supports_density_matrix:expr,
        pending_message = $pending_message:literal,
        timeout_message = $timeout_message:literal
    ) => {
        impl<C: $crate::credentials::CredentialSource, T: $crate::transport::HttpTransport>
            eg_quantum_core::backend::QuantumBackend for $backend<C, T>
        {
            fn backend_id(&self) -> eg_quantum_core::backend::BackendId {
                self.id.clone()
            }

            fn family(&self) -> eg_quantum_core::backend::BackendFamily {
                eg_quantum_core::backend::BackendFamily::Hardware
            }

            fn capabilities(&self) -> eg_quantum_core::backend::BackendCapabilities {
                eg_quantum_core::backend::BackendCapabilities {
                    supports_density_matrix: $supports_density_matrix,
                    supports_distributed: false,
                    supports_noise: true,
                    supports_gpu: false,
                    supports_mps: false,
                    supports_stabilizer: false,
                    is_exact_capable: false,
                    max_qubits_statevector: None,
                    max_qubits_density_matrix: None,
                    requires_hardware: true,
                }
            }

            fn submit(
                &self,
                program: &eg_quantum_core::ir::QuantumProgram,
                opts: &eg_quantum_core::backend::RunOptions,
            ) -> Result<eg_quantum_core::backend::JobHandle, eg_quantum_core::backend::BackendError>
            {
                let record = self
                    .execute_submit(program, opts)
                    .map_err(|error| error.into_backend_error(&self.id))?;
                Ok(self.jobs.insert(record))
            }

            fn poll(
                &self,
                job: eg_quantum_core::backend::JobHandle,
            ) -> Result<eg_quantum_core::backend::JobStatus, eg_quantum_core::backend::BackendError>
            {
                self.jobs.with_record(job, |record| {
                    self.client
                        .refresh_status(record)
                        .map_err(|error| error.into_backend_error(&self.id))?;
                    Ok(record.status.clone())
                })
            }

            fn result(
                &self,
                job: eg_quantum_core::backend::JobHandle,
            ) -> Result<
                eg_quantum_core::result::QuantumResult,
                eg_quantum_core::backend::BackendError,
            > {
                self.jobs.with_record(job, |record| {
                    if !matches!(
                        record.status,
                        eg_quantum_core::backend::JobStatus::Completed
                    ) {
                        return Err(eg_quantum_core::backend::BackendError::Execution(
                            $pending_message.to_string(),
                        ));
                    }
                    self.client
                        .fetch_result(&self.id, record)
                        .map_err(|error| error.into_backend_error(&self.id))
                })
            }

            fn cancel(
                &self,
                job: eg_quantum_core::backend::JobHandle,
            ) -> Result<(), eg_quantum_core::backend::BackendError> {
                self.jobs.with_record(job, |record| {
                    if record.is_terminal() {
                        return Ok(());
                    }
                    self.client
                        .cancel(record)
                        .map_err(|error| error.into_backend_error(&self.id))?;
                    record.status = eg_quantum_core::backend::JobStatus::Cancelled;
                    Ok(())
                })
            }

            fn run(
                &self,
                program: &eg_quantum_core::ir::QuantumProgram,
                opts: &eg_quantum_core::backend::RunOptions,
            ) -> Result<
                eg_quantum_core::result::QuantumResult,
                eg_quantum_core::backend::BackendError,
            > {
                $crate::registry::run_to_completion(
                    opts,
                    || self.submit(program, opts),
                    |job| self.poll(job),
                    |job| self.result(job),
                    |job| self.cancel(job),
                    $timeout_message,
                )
            }
        }
    };
}

pub(crate) use impl_hardware_backend;
