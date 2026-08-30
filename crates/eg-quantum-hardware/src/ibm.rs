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

use crate::{
    credentials::{CredentialSource, EnvCredentials},
    error::HardwareError,
    quota::{QuotaStatus, QuotaTracker, QuotaUnits},
    registry::{self, BackendState, HardwareJob, PreparedCircuit},
    transport::{Body, HttpRequest, HttpTransport, Method, ProviderHttp, ReqwestTransport},
};
use eg_quantum_core::backend::{BackendId, JobHandle, RunOptions};
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

const IBM_STATUS_MAPPING: registry::StatusMapping = registry::StatusMapping {
    provider: "IBM",
    status_noun: "job",
    queued: &["QUEUED", "INITIALIZING"],
    running: &["RUNNING"],
    completed: &["COMPLETED"],
    cancelled: &["CANCELLED"],
    failed: &[],
};

/// IBM Cloud IAM/Qiskit Runtime request client.
///
/// Authentication, service-CRN headers, endpoint construction, and IBM wire
/// translation belong together here. The backend remains responsible for the
/// Open Plan quota and the provider-independent local job registry.
struct IbmClient<C: CredentialSource, T: HttpTransport> {
    credentials: C,
    transport: T,
    api_base: String,
    iam_url: String,
}

impl<C: CredentialSource, T: HttpTransport> IbmClient<C, T> {
    fn new(credentials: C, transport: T) -> Self {
        IbmClient {
            credentials,
            transport,
            api_base: DEFAULT_API_BASE.to_string(),
            iam_url: DEFAULT_IAM_URL.to_string(),
        }
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
        ProviderHttp::new(&self.transport, "ibm-quantum").oauth_token(req, "IAM")
    }

    fn auth_headers(&self) -> Result<Vec<(String, String)>, HardwareError> {
        let token = self.iam_token()?;
        let crn = self.credentials.require(IBM_QUANTUM_INSTANCE_CRN_ENV)?;
        Ok(vec![
            ("Authorization".to_string(), format!("Bearer {token}")),
            ("Service-CRN".to_string(), crn),
        ])
    }

    fn job_request(&self, suffix: &str) -> Result<crate::transport::HttpResponse, HardwareError> {
        let req = HttpRequest {
            method: Method::Get,
            url: format!("{}/jobs/{suffix}", self.api_base),
            headers: self.auth_headers()?,
            body: None,
        };
        ProviderHttp::new(&self.transport, "ibm-quantum").send_checked(req, &[200])
    }

    fn submit(
        &self,
        backend_id: &BackendId,
        circuit: &PreparedCircuit,
        opts: &RunOptions,
    ) -> Result<String, HardwareError> {
        let mut headers = self.auth_headers()?;
        headers.push(("Content-Type".to_string(), "application/json".to_string()));
        // NOT the real Qiskit Runtime job-submission payload (that requires a PUB
        // (primitive unified bloc) built from an OpenQASM/QPY-serialized circuit --
        // see this crate's module docs on the Q2 dependency). This is the identity
        // envelope: enough for a real IBM API call to be REJECTED cleanly with a
        // clear "malformed program" error rather than silently degrading, while
        // still exercising auth, quota, transport, and job lifecycle machinery.
        let envelope = serde_json::json!({
            "program_id": "sampler",
            "backend": backend_id.to_string(),
            "shots": opts.shots.unwrap_or(1),
            "metadata": {
                "circuit_hash": circuit.hash.to_string(),
                "n_qubits": circuit.n_qubits,
            },
        });
        let req = HttpRequest {
            method: Method::Post,
            url: format!("{}/jobs", self.api_base),
            headers,
            body: Some(Body::Json(envelope)),
        };
        let response =
            ProviderHttp::new(&self.transport, "ibm-quantum").send_checked(req, &[200, 201])?;
        crate::registry::remote_id(&response.body, "id", "job response missing id")
    }

    fn cancel_request(
        &self,
        record: &HardwareJob,
    ) -> Result<crate::transport::HttpResponse, HardwareError> {
        let req = HttpRequest {
            method: Method::Delete,
            url: format!("{}/jobs/{}", self.api_base, record.remote_id),
            headers: self.auth_headers()?,
            body: None,
        };
        ProviderHttp::new(&self.transport, "ibm-quantum").send(req)
    }
}

impl<C: CredentialSource, T: HttpTransport> registry::ProviderClient for IbmClient<C, T> {
    fn request(
        &self,
        operation: registry::JobOperation,
        record: &HardwareJob,
    ) -> Result<crate::transport::HttpResponse, HardwareError> {
        let suffix = operation.remote_suffix(&record.remote_id, "results");
        if operation.is_cancel() {
            self.cancel_request(record)
        } else {
            self.job_request(&suffix)
        }
    }
}

pub struct IbmQuantumBackend<
    C: CredentialSource = EnvCredentials,
    T: HttpTransport = ReqwestTransport,
> {
    state: BackendState,
    client: IbmClient<C, T>,
    quota: QuotaTracker,
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
            state: BackendState::new("hardware-ibm"),
            client: IbmClient::new(credentials, transport),
            quota,
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
        self.state.quota_status_at_submit(job)
    }
}

impl<C: CredentialSource, T: HttpTransport> registry::FixedQuotaProvider
    for IbmQuantumBackend<C, T>
{
    fn quota_tracker(&self) -> &QuotaTracker {
        &self.quota
    }
}

impl<C: CredentialSource, T: HttpTransport> registry::ProviderRemoteSubmit
    for IbmQuantumBackend<C, T>
{
    fn submit_remote(
        &self,
        circuit: &PreparedCircuit,
        opts: &RunOptions,
    ) -> Result<String, HardwareError> {
        self.client.submit(&self.state.id, circuit, opts)
    }
}

crate::registry::impl_hardware_backend!(
    IbmQuantumBackend,
    supports_density_matrix = false,
    pending_message = "job has not completed yet -- call poll() until JobStatus::Completed",
    timeout_message = "run() timed out waiting for the IBM job to complete",
    status_mapping = &IBM_STATUS_MAPPING,
    result_mapping = registry::ResultMapping {
        counts_field: "counts",
        fidelity_field: Some("fidelity_hint"),
    },
);
