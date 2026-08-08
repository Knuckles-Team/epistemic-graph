//! [`AzureQuantumBackend`] -- Azure Quantum behind [`QuantumBackend`].
//!
//! Free surface: the free QDK (a local dev tool, not a network target -- irrelevant
//! to this adapter) plus "occasional new-workspace credits," which -- unlike IBM's
//! fixed 10-minute/28-day window or Braket's fixed 1-hour/30-day Free Tier -- is NOT
//! a fixed, predictable recurring quota. Auth: Azure AD OAuth2 client-credentials
//! grant (`POST https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token`,
//! form-encoded) for a bearer token, then Azure Quantum's workspace-scoped REST API.
//!
//! # Budget enforcement without a fixed provider quota
//!
//! The charter is explicit that budget enforcement is mandatory, not optional, for
//! every Q10 provider -- it does not carve out an exception for a provider whose free
//! tier has no fixed shape. Rather than silently defaulting to "unlimited" (which
//! would be the actual behaviour of naively skipping quota enforcement here), this
//! adapter requires an EXPLICIT operator-declared budget
//! ([`AZURE_QUANTUM_BUDGET_UNITS_ENV`] / [`AZURE_QUANTUM_BUDGET_WINDOW_DAYS_ENV`]) --
//! the unit is deliberately abstract (an operator-chosen proxy: job count, USD-cents,
//! whatever their Azure billing alerts are keyed on) since Azure's own credits are
//! not denominated in QPU-seconds the way IBM's/Braket's quotas are. **Submission is
//! refused outright, unconditionally, until that configuration is present** -- no
//! implicit unlimited access is ever granted by omission.

use crate::credentials::{CredentialSource, EnvCredentials};
use crate::error::HardwareError;
use crate::quota::{QuotaStatus, QuotaTracker, QuotaUnits};
use crate::transport::{Body, HttpRequest, HttpTransport, Method, ReqwestTransport};
use eg_quantum_core::backend::{
    BackendCapabilities, BackendError, BackendFamily, BackendId, JobHandle, JobStatus,
    QuantumBackend, RunOptions,
};
use eg_quantum_core::hash::CircuitHash;
use eg_quantum_core::ir::QuantumProgram;
use eg_quantum_core::result::{Formalism, Outcome, QuantumResult};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime};

pub const AZURE_QUANTUM_TENANT_ID_ENV: &str = "AZURE_QUANTUM_TENANT_ID";
pub const AZURE_QUANTUM_CLIENT_ID_ENV: &str = "AZURE_QUANTUM_CLIENT_ID";
pub const AZURE_QUANTUM_CLIENT_SECRET_ENV: &str = "AZURE_QUANTUM_CLIENT_SECRET";
pub const AZURE_QUANTUM_SUBSCRIPTION_ID_ENV: &str = "AZURE_QUANTUM_SUBSCRIPTION_ID";
pub const AZURE_QUANTUM_RESOURCE_GROUP_ENV: &str = "AZURE_QUANTUM_RESOURCE_GROUP";
pub const AZURE_QUANTUM_WORKSPACE_ENV: &str = "AZURE_QUANTUM_WORKSPACE";
pub const AZURE_QUANTUM_LOCATION_ENV: &str = "AZURE_QUANTUM_LOCATION";
/// See module docs: mandatory, operator-declared, abstract-unit budget. No default.
pub const AZURE_QUANTUM_BUDGET_UNITS_ENV: &str = "AZURE_QUANTUM_BUDGET_UNITS";
/// Rolling window (days) the above budget applies over. No default -- both must be
/// set together, or the backend refuses to submit at all.
pub const AZURE_QUANTUM_BUDGET_WINDOW_DAYS_ENV: &str = "AZURE_QUANTUM_BUDGET_WINDOW_DAYS";

const AAD_TOKEN_SCOPE: &str = "https://quantum.microsoft.com/.default";

struct JobRecord {
    remote_job_id: String,
    status: JobStatus,
    result: Option<QuantumResult>,
    quota_at_submit: QuotaStatus,
    circuit_hash: CircuitHash,
}

fn estimate_cost_units(opts: &RunOptions) -> QuotaUnits {
    QuotaUnits(opts.shots.unwrap_or(1).max(1))
}

pub struct AzureQuantumBackend<
    C: CredentialSource = EnvCredentials,
    T: HttpTransport = ReqwestTransport,
> {
    id: BackendId,
    credentials: C,
    transport: T,
    /// `None` until `AZURE_QUANTUM_BUDGET_UNITS`/`_WINDOW_DAYS` are both read
    /// successfully -- see module docs. Every submission re-checks this rather than
    /// caching a permanent refusal, so setting the env vars after process start (a
    /// config reload) is honoured on the next call.
    quota: Mutex<Option<QuotaTracker>>,
    /// Guards against re-reading `QuotaTracker::new` on every single submission once
    /// configuration IS present (constructing a fresh, empty `InMemoryQuotaStore`
    /// each call would silently reset usage tracking to zero every time -- exactly
    /// the bug the reserve-before-submit design exists to prevent). Populated once,
    /// the first time configuration is found valid.
    quota_initialized: OnceLock<()>,
    jobs: Mutex<HashMap<u64, JobRecord>>,
    next_handle: AtomicU64,
}

impl AzureQuantumBackend<EnvCredentials, ReqwestTransport> {
    pub fn new() -> Self {
        Self::with_parts(EnvCredentials, ReqwestTransport::default())
    }
}

impl Default for AzureQuantumBackend<EnvCredentials, ReqwestTransport> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: CredentialSource, T: HttpTransport> AzureQuantumBackend<C, T> {
    pub fn with_parts(credentials: C, transport: T) -> Self {
        AzureQuantumBackend {
            id: BackendId::from("hardware-azure"),
            credentials,
            transport,
            quota: Mutex::new(None),
            quota_initialized: OnceLock::new(),
            jobs: Mutex::new(HashMap::new()),
            next_handle: AtomicU64::new(0),
        }
    }

    /// `Ok(())` once a `QuotaTracker` is available (lazily built the first time
    /// `AZURE_QUANTUM_BUDGET_UNITS`/`_WINDOW_DAYS` resolve through `self.credentials`
    /// -- note this crate's `CredentialSource` is injectable, so a test using
    /// `StaticCredentials` supplies these the same way it supplies every other
    /// credential, no separate test-only hook needed); `Err` (mapped to
    /// `BackendError::ResourceLimit` at the trait boundary, per module docs) if no
    /// budget has ever been declared.
    fn ensure_quota_configured(&self) -> Result<(), HardwareError> {
        if self.quota_initialized.get().is_some() {
            return Ok(());
        }
        let units = self
            .credentials
            .require(AZURE_QUANTUM_BUDGET_UNITS_ENV)
            .map_err(|_| {
                HardwareError::BudgetNotConfigured(format!(
                    "Azure Quantum has no fixed free-tier quota; this adapter refuses to \
                     submit until an operator explicitly declares a budget via {} and {} \
                     (see azure.rs module docs) -- neither is set",
                    AZURE_QUANTUM_BUDGET_UNITS_ENV, AZURE_QUANTUM_BUDGET_WINDOW_DAYS_ENV
                ))
            })?
            .parse::<u64>()
            .map_err(|e| {
                HardwareError::BudgetNotConfigured(format!(
                    "{AZURE_QUANTUM_BUDGET_UNITS_ENV} must be a non-negative integer: {e}"
                ))
            })?;
        let window_days = self
            .credentials
            .require(AZURE_QUANTUM_BUDGET_WINDOW_DAYS_ENV)
            .map_err(|_| {
                HardwareError::BudgetNotConfigured(format!(
                    "{AZURE_QUANTUM_BUDGET_UNITS_ENV} is set but {AZURE_QUANTUM_BUDGET_WINDOW_DAYS_ENV} \
                     is not -- both are required together"
                ))
            })?
            .parse::<u64>()
            .map_err(|e| {
                HardwareError::BudgetNotConfigured(format!(
                    "{AZURE_QUANTUM_BUDGET_WINDOW_DAYS_ENV} must be a non-negative integer: {e}"
                ))
            })?;
        let mut guard = self.quota.lock().expect("quota mutex poisoned");
        if guard.is_none() {
            *guard = Some(QuotaTracker::new(
                "azure-quantum-operator-budget",
                "operator-units",
                Duration::from_secs(window_days * 86_400),
                QuotaUnits(units),
            ));
        }
        drop(guard);
        let _ = self.quota_initialized.set(());
        Ok(())
    }

    pub fn quota_status(&self) -> Option<QuotaStatus> {
        self.quota
            .lock()
            .expect("quota mutex poisoned")
            .as_ref()
            .map(|q| q.status(SystemTime::now()))
    }

    pub fn quota_status_at_submit(&self, job: JobHandle) -> Option<QuotaStatus> {
        self.jobs
            .lock()
            .expect("job store mutex poisoned")
            .get(&job.0)
            .map(|r| r.quota_at_submit)
    }

    fn api_base(&self) -> Result<String, HardwareError> {
        let location = self.credentials.require(AZURE_QUANTUM_LOCATION_ENV)?;
        let sub = self
            .credentials
            .require(AZURE_QUANTUM_SUBSCRIPTION_ID_ENV)?;
        let rg = self.credentials.require(AZURE_QUANTUM_RESOURCE_GROUP_ENV)?;
        let ws = self.credentials.require(AZURE_QUANTUM_WORKSPACE_ENV)?;
        Ok(format!(
            "https://{location}.quantum.azure.com/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Quantum/workspaces/{ws}"
        ))
    }

    fn aad_token(&self) -> Result<String, HardwareError> {
        let tenant = self.credentials.require(AZURE_QUANTUM_TENANT_ID_ENV)?;
        let client_id = self.credentials.require(AZURE_QUANTUM_CLIENT_ID_ENV)?;
        let client_secret = self.credentials.require(AZURE_QUANTUM_CLIENT_SECRET_ENV)?;
        let req = HttpRequest {
            method: Method::Post,
            url: format!("https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"),
            headers: vec![(
                "Content-Type".to_string(),
                "application/x-www-form-urlencoded".to_string(),
            )],
            body: Some(Body::Form(vec![
                ("grant_type".to_string(), "client_credentials".to_string()),
                ("client_id".to_string(), client_id),
                ("client_secret".to_string(), client_secret),
                ("scope".to_string(), AAD_TOKEN_SCOPE.to_string()),
            ])),
        };
        let resp = self
            .transport
            .send(req)
            .map_err(|e| HardwareError::Transport {
                provider: "azure-quantum",
                source: e,
            })?;
        if resp.status != 200 {
            return Err(HardwareError::ProviderRejected {
                provider: "azure-quantum",
                status: resp.status,
                body: resp.body.to_string(),
            });
        }
        resp.body
            .get("access_token")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .ok_or_else(|| {
                HardwareError::UnexpectedResponse(
                    "AAD token response missing access_token".to_string(),
                    None,
                )
            })
    }

    fn auth_header(&self) -> Result<(String, String), HardwareError> {
        Ok((
            "Authorization".to_string(),
            format!("Bearer {}", self.aad_token()?),
        ))
    }

    fn execute_submit(
        &self,
        program: &QuantumProgram,
        opts: &RunOptions,
    ) -> Result<JobRecord, HardwareError> {
        self.ensure_quota_configured()?;
        program
            .validate()
            .map_err(|e| HardwareError::InvalidProgram(e.to_string()))?;
        let circuit_hash = program
            .circuit_hash()
            .map_err(|e| HardwareError::InvalidProgram(e.to_string()))?;

        let cost = estimate_cost_units(opts);
        let quota_status = {
            let guard = self.quota.lock().expect("quota mutex poisoned");
            let tracker = guard
                .as_ref()
                .expect("ensure_quota_configured just guaranteed Some");
            tracker.try_reserve(cost, SystemTime::now())?
        };

        let api_base = self.api_base()?;
        let auth = self.auth_header()?;
        let headers = vec![
            auth,
            ("Content-Type".to_string(), "application/json".to_string()),
        ];

        // NOT the real Azure Quantum job-submission payload (requires provider-
        // specific input data, e.g. a QIR bitcode blob or OpenQASM3 -- same Q2
        // dependency noted in `ibm.rs`/`braket.rs` and the crate root docs).
        let envelope = serde_json::json!({
            "providerId": "quantinuum",
            "target": "quantinuum.sim.h1-1e",
            "itemType": "Job",
            "inputParams": {
                "shots": opts.shots.unwrap_or(1),
                "circuitHash": circuit_hash.to_hex(),
                "nQubits": program.n_qubits,
            },
        });

        let req = HttpRequest {
            method: Method::Post,
            url: format!("{api_base}/jobs"),
            headers,
            body: Some(Body::Json(envelope)),
        };
        let resp = self
            .transport
            .send(req)
            .map_err(|e| HardwareError::Transport {
                provider: "azure-quantum",
                source: e,
            })?;
        if resp.status != 200 && resp.status != 201 {
            return Err(HardwareError::ProviderRejected {
                provider: "azure-quantum",
                status: resp.status,
                body: resp.body.to_string(),
            });
        }
        let remote_job_id = resp
            .body
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                HardwareError::UnexpectedResponse("job response missing id".to_string(), None)
            })?
            .to_string();

        Ok(JobRecord {
            remote_job_id,
            status: JobStatus::Queued,
            result: None,
            quota_at_submit: quota_status,
            circuit_hash,
        })
    }

    fn refresh_status(&self, record: &mut JobRecord) -> Result<(), HardwareError> {
        if matches!(
            record.status,
            JobStatus::Completed | JobStatus::Failed(_) | JobStatus::Cancelled
        ) {
            return Ok(());
        }
        let api_base = self.api_base()?;
        let auth = self.auth_header()?;
        let req = HttpRequest {
            method: Method::Get,
            url: format!("{api_base}/jobs/{}", record.remote_job_id),
            headers: vec![auth],
            body: None,
        };
        let resp = self
            .transport
            .send(req)
            .map_err(|e| HardwareError::Transport {
                provider: "azure-quantum",
                source: e,
            })?;
        if resp.status != 200 {
            return Err(HardwareError::ProviderRejected {
                provider: "azure-quantum",
                status: resp.status,
                body: resp.body.to_string(),
            });
        }
        let status_str = resp
            .body
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        record.status = match status_str {
            "Waiting" => JobStatus::Queued,
            "Executing" => JobStatus::Running,
            "Succeeded" => JobStatus::Completed,
            "Cancelled" => JobStatus::Cancelled,
            "Failed" => JobStatus::Failed("Azure Quantum job reported Failed".to_string()),
            other => JobStatus::Failed(format!("unrecognized Azure job status {other:?}")),
        };
        Ok(())
    }

    fn fetch_result(&self, record: &mut JobRecord) -> Result<QuantumResult, HardwareError> {
        if let Some(r) = &record.result {
            return Ok(r.clone());
        }
        let api_base = self.api_base()?;
        let auth = self.auth_header()?;
        let req = HttpRequest {
            method: Method::Get,
            url: format!("{api_base}/jobs/{}/results", record.remote_job_id),
            headers: vec![auth],
            body: None,
        };
        let resp = self
            .transport
            .send(req)
            .map_err(|e| HardwareError::Transport {
                provider: "azure-quantum",
                source: e,
            })?;
        if resp.status != 200 {
            return Err(HardwareError::ProviderRejected {
                provider: "azure-quantum",
                status: resp.status,
                body: resp.body.to_string(),
            });
        }
        let counts: std::collections::BTreeMap<String, u64> = resp
            .body
            .get("counts")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_u64().map(|n| (k.clone(), n)))
                    .collect()
            })
            .unwrap_or_default();

        let result = QuantumResult::new_inexact(
            self.id.clone(),
            Formalism::Hardware,
            None,
            resp.body.get("shots").and_then(|v| v.as_u64()),
            record.circuit_hash,
            None,
            None,
            0,
            0,
            Outcome::Counts(counts),
        );
        record.result = Some(result.clone());
        Ok(result)
    }
}

impl<C: CredentialSource, T: HttpTransport> QuantumBackend for AzureQuantumBackend<C, T> {
    fn backend_id(&self) -> BackendId {
        self.id.clone()
    }

    fn family(&self) -> BackendFamily {
        BackendFamily::Hardware
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            supports_density_matrix: false,
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
        program: &QuantumProgram,
        opts: &RunOptions,
    ) -> Result<JobHandle, BackendError> {
        let record = self
            .execute_submit(program, opts)
            .map_err(|e| e.into_backend_error(&self.id))?;
        let handle = self.next_handle.fetch_add(1, Ordering::SeqCst);
        self.jobs
            .lock()
            .expect("job store mutex poisoned")
            .insert(handle, record);
        Ok(JobHandle(handle))
    }

    fn poll(&self, job: JobHandle) -> Result<JobStatus, BackendError> {
        let mut guard = self.jobs.lock().expect("job store mutex poisoned");
        let record = guard.get_mut(&job.0).ok_or(BackendError::UnknownJob)?;
        self.refresh_status(record)
            .map_err(|e| e.into_backend_error(&self.id))?;
        Ok(record.status.clone())
    }

    fn result(&self, job: JobHandle) -> Result<QuantumResult, BackendError> {
        let mut guard = self.jobs.lock().expect("job store mutex poisoned");
        let record = guard.get_mut(&job.0).ok_or(BackendError::UnknownJob)?;
        if !matches!(record.status, JobStatus::Completed) {
            return Err(BackendError::Execution(
                "job has not completed yet -- call poll() until JobStatus::Completed".to_string(),
            ));
        }
        self.fetch_result(record)
            .map_err(|e| e.into_backend_error(&self.id))
    }

    fn cancel(&self, job: JobHandle) -> Result<(), BackendError> {
        let mut guard = self.jobs.lock().expect("job store mutex poisoned");
        let record = guard.get_mut(&job.0).ok_or(BackendError::UnknownJob)?;
        if matches!(
            record.status,
            JobStatus::Completed | JobStatus::Failed(_) | JobStatus::Cancelled
        ) {
            return Ok(());
        }
        let api_base = self
            .api_base()
            .map_err(|e| e.into_backend_error(&self.id))?;
        let auth = self
            .auth_header()
            .map_err(|e| e.into_backend_error(&self.id))?;
        let req = HttpRequest {
            method: Method::Delete,
            url: format!("{api_base}/jobs/{}", record.remote_job_id),
            headers: vec![auth],
            body: None,
        };
        self.transport.send(req).map_err(|e| {
            HardwareError::Transport {
                provider: "azure-quantum",
                source: e,
            }
            .into_backend_error(&self.id)
        })?;
        record.status = JobStatus::Cancelled;
        Ok(())
    }

    fn run(
        &self,
        program: &QuantumProgram,
        opts: &RunOptions,
    ) -> Result<QuantumResult, BackendError> {
        let job = self.submit(program, opts)?;
        let deadline =
            std::time::Instant::now() + Duration::from_millis(opts.timeout_ms.unwrap_or(300_000));
        let mut backoff = Duration::from_millis(500);
        loop {
            match self.poll(job)? {
                JobStatus::Completed => return self.result(job),
                JobStatus::Failed(msg) => return Err(BackendError::Execution(msg)),
                JobStatus::Cancelled => return Err(BackendError::Cancelled),
                JobStatus::Queued | JobStatus::Running => {
                    if std::time::Instant::now() >= deadline {
                        let _ = self.cancel(job);
                        return Err(BackendError::ResourceLimit(
                            "run() timed out waiting for the Azure Quantum job to complete"
                                .to_string(),
                        ));
                    }
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(Duration::from_secs(10));
                }
            }
        }
    }
}
