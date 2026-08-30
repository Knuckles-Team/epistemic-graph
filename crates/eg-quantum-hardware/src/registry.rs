//! Shared provider job lifecycle for the hardware adapters.
//!
//! The provider modules own credentials, URLs, request signing, and wire-status
//! translation. This module owns only the provider-independent state machine
//! boundary: one local job value, one handle registry, program preparation, and
//! the synchronous polling policy shared by IBM, Braket, and Azure.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::credentials::CredentialSource;
use crate::error::HardwareError;
use crate::quota::{QuotaStatus, QuotaTracker, QuotaUnits};
use crate::transport::HttpResponse;
use eg_quantum_core::backend::{BackendError, BackendId, JobHandle, JobStatus, RunOptions};
use eg_quantum_core::hash::CircuitHash;
use eg_quantum_core::ir::QuantumProgram;
use eg_quantum_core::result::QuantumResult;

pub(crate) type HardwareResult = Result<QuantumResult, HardwareError>;
pub(crate) type JobSubmission = Result<HardwareJob, HardwareError>;

/// Operator-declared rolling budget configuration for providers without a fixed
/// public free-tier allowance.
pub(crate) struct ConfiguredQuota {
    tracker: Mutex<Option<QuotaTracker>>,
    initialized: OnceLock<()>,
}

impl Default for ConfiguredQuota {
    fn default() -> Self {
        ConfiguredQuota {
            tracker: Mutex::new(None),
            initialized: OnceLock::new(),
        }
    }
}

impl ConfiguredQuota {
    pub(crate) fn ensure_configured<C: CredentialSource>(
        &self,
        credentials: &C,
        provider_label: &str,
        units_env: &'static str,
        window_env: &'static str,
    ) -> Result<(), HardwareError> {
        if self.initialized.get().is_some() {
            return Ok(());
        }
        let units = credentials
            .require(units_env)
            .map_err(|_| {
                HardwareError::BudgetNotConfigured(format!(
                    "{provider_label} has no fixed free-tier quota; this adapter refuses to \
                 submit until an operator explicitly declares a budget via {units_env} and \
                 {window_env} (see azure.rs module docs) -- neither is set"
                ))
            })?
            .parse::<u64>()
            .map_err(|error| {
                HardwareError::BudgetNotConfigured(format!(
                    "{units_env} must be a non-negative integer: {error}"
                ))
            })?;
        let window_days = credentials
            .require(window_env)
            .map_err(|_| {
                HardwareError::BudgetNotConfigured(format!(
                    "{units_env} is set but {window_env} is not -- both are required together"
                ))
            })?
            .parse::<u64>()
            .map_err(|error| {
                HardwareError::BudgetNotConfigured(format!(
                    "{window_env} must be a non-negative integer: {error}"
                ))
            })?;

        let mut guard = self.tracker.lock().expect("quota mutex poisoned");
        if guard.is_none() {
            *guard = Some(QuotaTracker::new(
                "azure-quantum-operator-budget",
                "operator-units",
                Duration::from_secs(window_days * 86_400),
                QuotaUnits(units),
            ));
        }
        drop(guard);
        let _ = self.initialized.set(());
        Ok(())
    }

    pub(crate) fn reserve(&self, cost: QuotaUnits) -> Result<QuotaStatus, HardwareError> {
        let guard = self.tracker.lock().expect("quota mutex poisoned");
        let tracker = guard
            .as_ref()
            .expect("ConfiguredQuota::ensure_configured must run before reserve");
        Ok(tracker.try_reserve(cost, std::time::SystemTime::now())?)
    }

    pub(crate) fn status(&self) -> Option<QuotaStatus> {
        self.tracker
            .lock()
            .expect("quota mutex poisoned")
            .as_ref()
            .map(|quota| quota.status(std::time::SystemTime::now()))
    }
}

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

/// Provider-neutral identity and local job storage shared by every adapter.
pub(crate) struct BackendState {
    pub(crate) id: BackendId,
    pub(crate) jobs: HardwareJobRegistry,
}

impl BackendState {
    pub(crate) fn new(id: &'static str) -> Self {
        BackendState {
            id: BackendId::from(id),
            jobs: HardwareJobRegistry::default(),
        }
    }

    pub(crate) fn quota_status_at_submit(&self, job: JobHandle) -> Option<QuotaStatus> {
        self.jobs.quota_status_at_submit(job)
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

pub(crate) fn submit_with_quota<R, S>(
    program: &QuantumProgram,
    opts: &RunOptions,
    reserve: R,
    submit: S,
) -> Result<HardwareJob, HardwareError>
where
    R: FnOnce(QuotaUnits) -> Result<QuotaStatus, HardwareError>,
    S: FnOnce(&PreparedCircuit, &RunOptions) -> Result<String, HardwareError>,
{
    let circuit = PreparedCircuit::from_program(program)?;
    let quota_status = reserve(QuotaUnits(opts.shots.unwrap_or(1).max(1)))?;
    let remote_id = submit(&circuit, opts)?;
    Ok(queued_job(remote_id, quota_status, circuit.hash))
}

/// Shared submission lifecycle for providers with provider-specific reservation
/// and remote-submit operations.
pub(crate) trait ProviderSubmission {
    fn before_submit(&self) -> Result<(), HardwareError> {
        Ok(())
    }

    fn reserve_submission(&self, cost: QuotaUnits) -> Result<QuotaStatus, HardwareError>;

    fn submit_remote(
        &self,
        circuit: &PreparedCircuit,
        opts: &RunOptions,
    ) -> Result<String, HardwareError>;

    fn execute_submit(&self, program: &QuantumProgram, opts: &RunOptions) -> JobSubmission {
        self.before_submit()?;
        submit_with_quota(
            program,
            opts,
            |cost| self.reserve_submission(cost),
            |circuit, opts| self.submit_remote(circuit, opts),
        )
    }
}

/// Fixed-budget providers share reservation semantics; remote submission stays
/// in its own seam because each provider's request payload is different.
pub(crate) trait FixedQuotaProvider {
    fn quota_tracker(&self) -> &QuotaTracker;
}

pub(crate) trait ProviderRemoteSubmit {
    fn submit_remote(
        &self,
        circuit: &PreparedCircuit,
        opts: &RunOptions,
    ) -> Result<String, HardwareError>;
}

impl<T: FixedQuotaProvider + ProviderRemoteSubmit> ProviderSubmission for T {
    fn reserve_submission(&self, cost: QuotaUnits) -> Result<QuotaStatus, HardwareError> {
        Ok(self
            .quota_tracker()
            .try_reserve(cost, std::time::SystemTime::now())?)
    }

    fn submit_remote(
        &self,
        circuit: &PreparedCircuit,
        opts: &RunOptions,
    ) -> Result<String, HardwareError> {
        ProviderRemoteSubmit::submit_remote(self, circuit, opts)
    }
}

/// Provider-specific wire labels consumed by the shared status lifecycle.
pub(crate) struct StatusMapping {
    pub(crate) provider: &'static str,
    pub(crate) status_noun: &'static str,
    pub(crate) queued: &'static [&'static str],
    pub(crate) running: &'static [&'static str],
    pub(crate) completed: &'static [&'static str],
    pub(crate) cancelled: &'static [&'static str],
    pub(crate) failed: &'static [(&'static str, &'static str)],
}

impl StatusMapping {
    fn status(&self, value: &serde_json::Value) -> JobStatus {
        let status = value
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if self.queued.contains(&status) {
            return JobStatus::Queued;
        }
        if self.running.contains(&status) {
            return JobStatus::Running;
        }
        if self.completed.contains(&status) {
            return JobStatus::Completed;
        }
        if self.cancelled.contains(&status) {
            return JobStatus::Cancelled;
        }
        if let Some((_, message)) = self.failed.iter().find(|(label, _)| *label == status) {
            return JobStatus::Failed((*message).to_string());
        }
        JobStatus::Failed(format!(
            "unrecognized {} {} status {status:?}",
            self.provider, self.status_noun
        ))
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ResultMapping {
    pub(crate) counts_field: &'static str,
    pub(crate) fidelity_field: Option<&'static str>,
}

#[derive(Clone, Copy)]
pub(crate) enum JobOperation {
    Status,
    Result,
    Cancel,
}

impl JobOperation {
    pub(crate) fn is_cancel(self) -> bool {
        matches!(self, JobOperation::Cancel)
    }

    pub(crate) fn remote_suffix(self, remote_id: &str, result_suffix: &str) -> String {
        match self {
            JobOperation::Result => format!("{remote_id}/{result_suffix}"),
            JobOperation::Status | JobOperation::Cancel => remote_id.to_string(),
        }
    }
}

/// Provider request seam with a shared local lifecycle.
///
/// Implementations only describe how one provider performs each wire operation;
/// the status cache, result cache, and cancellation bookkeeping stay here so the
/// adapters cannot drift in their terminal-state handling.
pub(crate) trait ProviderClient {
    fn request(
        &self,
        operation: JobOperation,
        record: &HardwareJob,
    ) -> Result<HttpResponse, HardwareError>;

    fn refresh_status(
        &self,
        record: &mut HardwareJob,
        mapping: &'static StatusMapping,
    ) -> Result<(), HardwareError> {
        refresh_status(
            record,
            |record| self.request(JobOperation::Status, record),
            mapping,
        )
    }

    fn fetch_result(
        &self,
        backend_id: &BackendId,
        record: &mut HardwareJob,
        mapping: ResultMapping,
    ) -> HardwareResult {
        fetch_result(
            backend_id,
            record,
            |record| self.request(JobOperation::Result, record),
            mapping,
        )
    }

    fn cancel(&self, record: &HardwareJob) -> Result<(), HardwareError> {
        self.request(JobOperation::Cancel, record).map(|_| ())
    }
}

/// Fetch and translate one provider status while retaining the terminal-state
/// short circuit owned by [`HardwareJob::refresh_with`].
pub(crate) fn refresh_status<F>(
    record: &mut HardwareJob,
    request: F,
    mapping: &'static StatusMapping,
) -> Result<(), HardwareError>
where
    F: FnOnce(&HardwareJob) -> Result<HttpResponse, HardwareError>,
{
    record.refresh_with(|record| {
        let response = request(record)?;
        Ok(mapping.status(&response.body))
    })
}

/// Fetch and translate one provider result while retaining the result cache owned
/// by [`HardwareJob::resolve_result`].
pub(crate) fn fetch_result<F>(
    backend_id: &BackendId,
    record: &mut HardwareJob,
    request: F,
    mapping: ResultMapping,
) -> Result<QuantumResult, HardwareError>
where
    F: FnOnce(&HardwareJob) -> Result<HttpResponse, HardwareError>,
{
    record.resolve_result(|record| {
        let response = request(record)?;
        Ok(hardware_result(
            backend_id,
            record,
            &response.body,
            mapping.counts_field,
            mapping.fidelity_field,
        ))
    })
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
        timeout_message = $timeout_message:literal,
        status_mapping = $status_mapping:expr,
        result_mapping = $result_mapping:expr
    ) => {
        impl<C: $crate::credentials::CredentialSource, T: $crate::transport::HttpTransport>
            eg_quantum_core::backend::QuantumBackend for $backend<C, T>
        {
            fn backend_id(&self) -> eg_quantum_core::backend::BackendId {
                self.state.id.clone()
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
                let record =
                    $crate::registry::ProviderSubmission::execute_submit(self, program, opts)
                        .map_err(|error| error.into_backend_error(&self.state.id))?;
                Ok(self.state.jobs.insert(record))
            }

            fn poll(
                &self,
                job: eg_quantum_core::backend::JobHandle,
            ) -> Result<eg_quantum_core::backend::JobStatus, eg_quantum_core::backend::BackendError>
            {
                self.state.jobs.with_record(job, |record| {
                    $crate::registry::ProviderClient::refresh_status(
                        &self.client,
                        record,
                        $status_mapping,
                    )
                    .map_err(|error| error.into_backend_error(&self.state.id))?;
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
                self.state.jobs.with_record(job, |record| {
                    if !matches!(
                        record.status,
                        eg_quantum_core::backend::JobStatus::Completed
                    ) {
                        return Err(eg_quantum_core::backend::BackendError::Execution(
                            $pending_message.to_string(),
                        ));
                    }
                    $crate::registry::ProviderClient::fetch_result(
                        &self.client,
                        &self.state.id,
                        record,
                        $result_mapping,
                    )
                    .map_err(|error| error.into_backend_error(&self.state.id))
                })
            }

            fn cancel(
                &self,
                job: eg_quantum_core::backend::JobHandle,
            ) -> Result<(), eg_quantum_core::backend::BackendError> {
                self.state.jobs.with_record(job, |record| {
                    if record.is_terminal() {
                        return Ok(());
                    }
                    $crate::registry::ProviderClient::cancel(&self.client, record)
                        .map_err(|error| error.into_backend_error(&self.state.id))?;
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
