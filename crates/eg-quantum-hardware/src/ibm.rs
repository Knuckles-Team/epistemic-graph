//! [`IbmQuantumBackend`] -- IBM Quantum (Open Plan) behind [`QuantumBackend`].
//!
//! Free surface: ~10 QPU-runtime minutes per rolling 28-day window, unlimited cloud
//! simulators, access to 100+ qubit Heron systems. Auth: IBM Cloud IAM API-key
//! exchange (`POST {iam_url}` with the `apikey` grant, form-encoded) for a bearer
//! token, then Qiskit Runtime REST calls carry `Authorization: Bearer <token>` plus
//! an `Service-CRN` header naming the target service instance.
//!
//! See the crate root docs for the "why circuit-payload translation is not fully
//! wired here" scope boundary (depends on Q2's OpenQASM export).

use crate::credentials::{CredentialSource, EnvCredentials};
use crate::error::HardwareError;
use crate::quota::{QuotaStatus, QuotaTracker, QuotaUnits};
use crate::transport::{Body, HttpRequest, HttpTransport, Method, ReqwestTransport};
use eg_quantum_core::backend::{
    BackendCapabilities, BackendError, BackendFamily, BackendId, JobHandle, JobStatus,
    QuantumBackend, RunOptions,
};
use eg_quantum_core::ir::QuantumProgram;
use eg_quantum_core::result::{Formalism, Outcome, QuantumResult};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

/// The IBM Cloud API key backing the Open Plan account. Populated from OpenBao via
/// external-secrets at deploy time -- see `credentials` module docs.
pub const IBM_QUANTUM_API_KEY_ENV: &str = "IBM_QUANTUM_API_KEY";
/// The Cloud Resource Name of the target Qiskit Runtime service instance.
pub const IBM_QUANTUM_INSTANCE_CRN_ENV: &str = "IBM_QUANTUM_INSTANCE_CRN";
/// Optional override of the IAM token endpoint (test/regional deployments).
pub const IBM_QUANTUM_IAM_URL_ENV: &str = "IBM_QUANTUM_IAM_URL";
/// Optional override of the Qiskit Runtime API base URL.
pub const IBM_QUANTUM_API_BASE_ENV: &str = "IBM_QUANTUM_API_BASE";

const DEFAULT_IAM_URL: &str = "https://iam.cloud.ibm.com/identity/token";
const DEFAULT_API_BASE: &str = "https://quantum.cloud.ibm.com/api/v1";

/// Open Plan budget: ~10 QPU-runtime minutes per rolling 28-day window.
const OPEN_PLAN_WINDOW: Duration = Duration::from_secs(28 * 86_400);
const OPEN_PLAN_LIMIT_SECONDS: u64 = 600;

struct JobRecord {
    remote_job_id: String,
    status: JobStatus,
    result: Option<QuantumResult>,
    quota_at_submit: QuotaStatus,
    /// The hash of the program AS SUBMITTED, computed locally (not parsed back from
    /// any provider response -- `CircuitHash` has no public constructor other than
    /// `QuantumProgram::circuit_hash()`, by design, so a result's `circuit_hash`
    /// always traces to a real local `QuantumProgram` value, never a
    /// provider-supplied string).
    circuit_hash: eg_quantum_core::hash::CircuitHash,
}

/// A dense-per-shot-second cost model: the reserved budget for a submission is
/// `shots` (default 1) since IBM's Open Plan meters wall-clock QPU seconds, not
/// shots, and this crate has no pre-execution timing oracle -- 1 second per shot is
/// a deliberately conservative (over-, not under-) estimate; see `quota.rs`'s
/// `try_reserve` docs on why over-counting is the safe direction to be wrong in.
fn estimate_cost_seconds(opts: &RunOptions) -> QuotaUnits {
    QuotaUnits(opts.shots.unwrap_or(1).max(1))
}

pub struct IbmQuantumBackend<
    C: CredentialSource = EnvCredentials,
    T: HttpTransport = ReqwestTransport,
> {
    id: BackendId,
    credentials: C,
    transport: T,
    api_base: String,
    iam_url: String,
    quota: QuotaTracker,
    jobs: Mutex<HashMap<u64, JobRecord>>,
    next_handle: AtomicU64,
}

impl IbmQuantumBackend<EnvCredentials, ReqwestTransport> {
    /// The constructor every real caller uses: environment-sourced credentials
    /// (OpenBao-populated), a real HTTP transport, and the Open Plan's default
    /// 600-second/28-day budget.
    pub fn new() -> Self {
        Self::with_parts(
            EnvCredentials,
            ReqwestTransport::default(),
            QuotaTracker::new(
                "ibm-quantum-open-plan",
                "qpu-seconds",
                OPEN_PLAN_WINDOW,
                QuotaUnits(OPEN_PLAN_LIMIT_SECONDS),
            ),
        )
    }
}

impl Default for IbmQuantumBackend<EnvCredentials, ReqwestTransport> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: CredentialSource, T: HttpTransport> IbmQuantumBackend<C, T> {
    /// Test/advanced constructor: inject credentials, transport, and quota tracker
    /// directly (e.g. `StaticCredentials` + `MockTransport` + a tiny test budget).
    pub fn with_parts(credentials: C, transport: T, quota: QuotaTracker) -> Self {
        IbmQuantumBackend {
            id: BackendId::from("hardware-ibm"),
            credentials,
            transport,
            api_base: DEFAULT_API_BASE.to_string(),
            iam_url: DEFAULT_IAM_URL.to_string(),
            quota,
            jobs: Mutex::new(HashMap::new()),
            next_handle: AtomicU64::new(0),
        }
    }

    /// Read-only accessor for the current budget snapshot -- the Q10 answer to
    /// "surface remaining budget through the result metadata" (see `quota.rs` module
    /// docs for why this lives here rather than inside `QuantumResult` itself).
    pub fn quota_status(&self) -> QuotaStatus {
        self.quota.status(SystemTime::now())
    }

    /// The budget snapshot recorded at the moment a specific job was submitted, if
    /// that job handle is known.
    pub fn quota_status_at_submit(&self, job: JobHandle) -> Option<QuotaStatus> {
        self.jobs
            .lock()
            .expect("job store mutex poisoned")
            .get(&job.0)
            .map(|r| r.quota_at_submit)
    }

    fn iam_token(&self) -> Result<String, HardwareError> {
        let api_key = self.credentials.require(IBM_QUANTUM_API_KEY_ENV)?;
        let req = HttpRequest {
            method: Method::Post,
            url: self.iam_url.clone(),
            headers: vec![(
                "Content-Type".to_string(),
                "application/x-www-form-urlencoded".to_string(),
            )],
            body: Some(Body::Form(vec![
                (
                    "grant_type".to_string(),
                    "urn:ibm:params:oauth:grant-type:apikey".to_string(),
                ),
                ("apikey".to_string(), api_key),
            ])),
        };
        let resp = self
            .transport
            .send(req)
            .map_err(|e| HardwareError::Transport {
                provider: "ibm-quantum",
                source: e,
            })?;
        if resp.status != 200 {
            return Err(HardwareError::ProviderRejected {
                provider: "ibm-quantum",
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
                    "IAM token response missing access_token".to_string(),
                    None,
                )
            })
    }

    fn auth_headers(&self) -> Result<Vec<(String, String)>, HardwareError> {
        let token = self.iam_token()?;
        let crn = self.credentials.require(IBM_QUANTUM_INSTANCE_CRN_ENV)?;
        Ok(vec![
            ("Authorization".to_string(), format!("Bearer {token}")),
            ("Service-CRN".to_string(), crn),
        ])
    }

    fn execute_submit(
        &self,
        program: &QuantumProgram,
        opts: &RunOptions,
    ) -> Result<JobRecord, HardwareError> {
        program
            .validate()
            .map_err(|e| HardwareError::InvalidProgram(e.to_string()))?;
        let circuit_hash = program
            .circuit_hash()
            .map_err(|e| HardwareError::InvalidProgram(e.to_string()))?;

        let cost = estimate_cost_seconds(opts);
        let quota_status = self.quota.try_reserve(cost, SystemTime::now())?;

        let mut headers = self.auth_headers()?;
        headers.push(("Content-Type".to_string(), "application/json".to_string()));

        // NOT the real Qiskit Runtime job-submission payload (that requires a PUB
        // (primitive unified bloc) built from an OpenQASM/QPY-serialized circuit --
        // see this crate's module docs on the Q2 dependency). This is the identity
        // envelope: enough for a real IBM API call to be REJECTED cleanly with a
        // clear "malformed program" error rather than silently degrading, while
        // still exercising the full auth/quota/transport/job-store machinery this
        // lane owns.
        let envelope = serde_json::json!({
            "program_id": "sampler",
            "backend": self.id.to_string(),
            "shots": opts.shots.unwrap_or(1),
            "metadata": {
                "circuit_hash": circuit_hash.to_string(),
                "n_qubits": program.n_qubits,
            },
        });

        let req = HttpRequest {
            method: Method::Post,
            url: format!("{}/jobs", self.api_base),
            headers,
            body: Some(Body::Json(envelope)),
        };
        let resp = self
            .transport
            .send(req)
            .map_err(|e| HardwareError::Transport {
                provider: "ibm-quantum",
                source: e,
            })?;
        if resp.status != 200 && resp.status != 201 {
            return Err(HardwareError::ProviderRejected {
                provider: "ibm-quantum",
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
        let headers = self.auth_headers()?;
        let req = HttpRequest {
            method: Method::Get,
            url: format!("{}/jobs/{}", self.api_base, record.remote_job_id),
            headers,
            body: None,
        };
        let resp = self
            .transport
            .send(req)
            .map_err(|e| HardwareError::Transport {
                provider: "ibm-quantum",
                source: e,
            })?;
        if resp.status != 200 {
            return Err(HardwareError::ProviderRejected {
                provider: "ibm-quantum",
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
            "QUEUED" | "INITIALIZING" => JobStatus::Queued,
            "RUNNING" => JobStatus::Running,
            "COMPLETED" => JobStatus::Completed,
            "CANCELLED" => JobStatus::Cancelled,
            other => JobStatus::Failed(format!("unrecognized IBM job status {other:?}")),
        };
        Ok(())
    }

    fn fetch_result(&self, record: &mut JobRecord) -> Result<QuantumResult, HardwareError> {
        if let Some(r) = &record.result {
            return Ok(r.clone());
        }
        let headers = self.auth_headers()?;
        let req = HttpRequest {
            method: Method::Get,
            url: format!("{}/jobs/{}/results", self.api_base, record.remote_job_id),
            headers,
            body: None,
        };
        let resp = self
            .transport
            .send(req)
            .map_err(|e| HardwareError::Transport {
                provider: "ibm-quantum",
                source: e,
            })?;
        if resp.status != 200 {
            return Err(HardwareError::ProviderRejected {
                provider: "ibm-quantum",
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
            resp.body.get("fidelity_hint").and_then(|v| v.as_f64()),
            0,
            0,
            Outcome::Counts(counts),
        );
        record.result = Some(result.clone());
        Ok(result)
    }
}

impl<C: CredentialSource, T: HttpTransport> QuantumBackend for IbmQuantumBackend<C, T> {
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
            // Per this crate's module docs: the whole Hardware family reports
            // false, unconditionally -- even for IBM's unlimited cloud-simulator
            // targets.
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
        let headers = self
            .auth_headers()
            .map_err(|e| e.into_backend_error(&self.id))?;
        let req = HttpRequest {
            method: Method::Delete,
            url: format!("{}/jobs/{}", self.api_base, record.remote_job_id),
            headers,
            body: None,
        };
        self.transport.send(req).map_err(|e| {
            HardwareError::Transport {
                provider: "ibm-quantum",
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
        // Bounded polling loop -- a real QPU queue is minutes-to-hours (see
        // `quantum-external-providers.md` ss1.1); this convenience method is meant for
        // callers who accept blocking, not for the async-job-plane path Q5 will
        // eventually provide (submit/poll are always available directly for that).
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
                            "run() timed out waiting for the IBM job to complete".to_string(),
                        ));
                    }
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(Duration::from_secs(10));
                }
            }
        }
    }
}
