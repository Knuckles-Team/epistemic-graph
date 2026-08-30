//! [`BraketBackend`] -- Amazon Braket behind [`QuantumBackend`].
//!
//! Free surface: unlimited local simulation (which this crate does NOT proxy through
//! here -- an operator wanting a free local simulator already has `eg-quantum-sim`,
//! the default backend this whole lane exists to stay secondary to) plus ~1
//! on-demand-cloud-simulator-hour/month (SV1/DM1/TN1) under the AWS Free Tier; real
//! QPU access is paid, and this adapter does not distinguish target device types for
//! quota purposes -- see crate root docs on why the whole `Hardware` family is
//! treated uniformly as never-exact and budget-tracked.
//!
//! Auth: AWS Signature Version 4 (see `sigv4.rs`) over Braket's control-plane REST
//! API (`braket.{region}.amazonaws.com`), NOT a bearer token -- the one respect in
//! which this adapter's transport differs structurally from `ibm`/`azure`.

use crate::credentials::{CredentialSource, EnvCredentials};
use crate::error::HardwareError;
use crate::quota::{QuotaStatus, QuotaTracker, QuotaUnits};
use crate::registry::{HardwareJob, HardwareJobRegistry, PreparedCircuit};
use crate::sigv4::{self, AwsCredentials};
use crate::transport::{Body, HttpRequest, HttpTransport, Method, ProviderHttp, ReqwestTransport};
use eg_quantum_core::backend::{BackendId, JobHandle, JobStatus, RunOptions};
use eg_quantum_core::ir::QuantumProgram;
use eg_quantum_core::result::QuantumResult;
use std::time::{Duration, SystemTime};

pub const AWS_BRAKET_ACCESS_KEY_ID_ENV: &str = "AWS_BRAKET_ACCESS_KEY_ID";
pub const AWS_BRAKET_SECRET_ACCESS_KEY_ENV: &str = "AWS_BRAKET_SECRET_ACCESS_KEY";
/// Optional: only set when using STS-issued temporary credentials.
pub const AWS_BRAKET_SESSION_TOKEN_ENV: &str = "AWS_BRAKET_SESSION_TOKEN";
pub const AWS_BRAKET_REGION_ENV: &str = "AWS_BRAKET_REGION";

const DEFAULT_REGION: &str = "us-east-1";
/// AWS Free Tier's on-demand cloud-simulator allowance: ~1 hour/month. Modeled as a
/// 30-day rolling window (this crate's quota tracker is calendar-agnostic; 30 days is
/// the same kind of documented approximation IBM's own "28-day" window already is).
const FREE_TIER_WINDOW: Duration = Duration::from_secs(30 * 86_400);
const FREE_TIER_LIMIT_SECONDS: u64 = 3_600;

fn estimate_cost_seconds(opts: &RunOptions) -> QuotaUnits {
    // Same conservative per-shot-second proxy as `ibm.rs` -- see that module's
    // `estimate_cost_seconds` docs.
    QuotaUnits(opts.shots.unwrap_or(1).max(1))
}

/// Braket-specific endpoint and SigV4 client.
///
/// The backend retains only provider-neutral identity, quota, and job state. This
/// client owns the AWS credential lookup, endpoint selection, signing, and wire
/// requests because those responsibilities are inseparable from Braket's API.
struct BraketClient<C: CredentialSource, T: HttpTransport> {
    credentials: C,
    transport: T,
    region: String,
}

impl<C: CredentialSource, T: HttpTransport> BraketClient<C, T> {
    fn new(credentials: C, transport: T) -> Self {
        BraketClient {
            credentials,
            transport,
            region: DEFAULT_REGION.to_string(),
        }
    }

    fn host(&self) -> String {
        let region = self
            .credentials
            .optional(AWS_BRAKET_REGION_ENV)
            .unwrap_or_else(|| self.region.clone());
        format!("braket.{region}.amazonaws.com")
    }

    fn aws_credentials(&self) -> Result<AwsCredentials, HardwareError> {
        Ok(AwsCredentials {
            access_key_id: self.credentials.require(AWS_BRAKET_ACCESS_KEY_ID_ENV)?,
            secret_access_key: self.credentials.require(AWS_BRAKET_SECRET_ACCESS_KEY_ENV)?,
            session_token: self.credentials.optional(AWS_BRAKET_SESSION_TOKEN_ENV),
            region: self
                .credentials
                .optional(AWS_BRAKET_REGION_ENV)
                .unwrap_or_else(|| self.region.clone()),
        })
    }

    fn signed_headers(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> Result<Vec<(String, String)>, HardwareError> {
        let creds = self.aws_credentials()?;
        let host = self.host();
        let signed = sigv4::sign(
            &creds,
            "braket",
            method,
            &host,
            path,
            "",
            body,
            SystemTime::now(),
        );
        let mut headers = vec![
            ("Host".to_string(), host),
            ("X-Amz-Date".to_string(), signed.x_amz_date),
            (
                "X-Amz-Content-Sha256".to_string(),
                signed.x_amz_content_sha256,
            ),
            ("Authorization".to_string(), signed.authorization),
            ("Content-Type".to_string(), "application/json".to_string()),
        ];
        if let Some(token) = signed.x_amz_security_token {
            headers.push(("X-Amz-Security-Token".to_string(), token));
        }
        Ok(headers)
    }

    fn submit(
        &self,
        circuit: &PreparedCircuit,
        opts: &RunOptions,
    ) -> Result<String, HardwareError> {
        // NOT the real CreateQuantumTask payload (requires an OpenQASM3 `action`
        // document -- same Q2 dependency noted in `ibm.rs` and the crate root docs).
        // This envelope carries the circuit's identity only.
        let envelope = serde_json::json!({
            "deviceArn": "arn:aws:braket:::device/quantum-simulator/amazon/sv1",
            "shots": opts.shots.unwrap_or(1),
            "clientToken": circuit.hash.to_hex(),
        });
        let body_bytes = serde_json::to_vec(&envelope).map_err(|e| {
            HardwareError::UnexpectedResponse(format!("failed to encode envelope: {e}"), None)
        })?;
        let headers = self.signed_headers("POST", "/quantum-task", &body_bytes)?;
        let req = HttpRequest {
            method: Method::Post,
            url: format!("https://{}/quantum-task", self.host()),
            headers,
            body: Some(Body::Json(envelope)),
        };
        let response =
            ProviderHttp::new(&self.transport, "aws-braket").send_checked(req, &[200, 201])?;
        crate::registry::remote_id(
            &response.body,
            "quantumTaskArn",
            "CreateQuantumTask response missing quantumTaskArn",
        )
    }

    fn task_request(
        &self,
        method: Method,
        path: &str,
    ) -> Result<crate::transport::HttpResponse, HardwareError> {
        let verb = match method {
            Method::Get => "GET",
            Method::Post => "POST",
            Method::Put => "PUT",
            Method::Delete => "DELETE",
        };
        let headers = self.signed_headers(verb, path, b"")?;
        let req = HttpRequest {
            method,
            url: format!("https://{}{path}", self.host()),
            headers,
            body: None,
        };
        ProviderHttp::new(&self.transport, "aws-braket").send_checked(req, &[200])
    }

    fn refresh_status(&self, record: &mut HardwareJob) -> Result<(), HardwareError> {
        record.refresh_with(|record| {
            let path = format!("/quantum-task/{}", record.remote_id);
            let response = self.task_request(Method::Get, &path)?;
            let status = response
                .body
                .get("status")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            Ok(match status {
                "CREATED" | "QUEUED" => JobStatus::Queued,
                "RUNNING" => JobStatus::Running,
                "COMPLETED" => JobStatus::Completed,
                "CANCELLED" => JobStatus::Cancelled,
                "FAILED" => JobStatus::Failed("Braket task reported FAILED".to_string()),
                other => JobStatus::Failed(format!("unrecognized Braket task status {other:?}")),
            })
        })
    }

    fn fetch_result(
        &self,
        backend_id: &BackendId,
        record: &mut HardwareJob,
    ) -> Result<QuantumResult, HardwareError> {
        record.resolve_result(|record| {
            let path = format!("/quantum-task/{}/result", record.remote_id);
            let response = self.task_request(Method::Get, &path)?;
            Ok(crate::registry::hardware_result(
                backend_id,
                record,
                &response.body,
                "measurementCounts",
                None,
            ))
        })
    }

    fn cancel(&self, record: &HardwareJob) -> Result<(), HardwareError> {
        let path = format!("/quantum-task/{}/cancel", record.remote_id);
        self.task_request(Method::Put, &path).map(|_| ())
    }
}

pub struct BraketBackend<C: CredentialSource = EnvCredentials, T: HttpTransport = ReqwestTransport>
{
    id: BackendId,
    client: BraketClient<C, T>,
    quota: QuotaTracker,
    jobs: HardwareJobRegistry,
}

impl BraketBackend<EnvCredentials, ReqwestTransport> {
    pub fn new() -> Self {
        Self::with_parts(
            EnvCredentials,
            ReqwestTransport::default(),
            QuotaTracker::new(
                "aws-braket-free-tier",
                "simulator-seconds",
                FREE_TIER_WINDOW,
                QuotaUnits(FREE_TIER_LIMIT_SECONDS),
            ),
        )
    }
}

impl Default for BraketBackend<EnvCredentials, ReqwestTransport> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: CredentialSource, T: HttpTransport> BraketBackend<C, T> {
    pub fn with_parts(credentials: C, transport: T, quota: QuotaTracker) -> Self {
        BraketBackend {
            id: BackendId::from("hardware-braket"),
            client: BraketClient::new(credentials, transport),
            quota,
            jobs: HardwareJobRegistry::default(),
        }
    }

    pub fn quota_status(&self) -> QuotaStatus {
        self.quota.status(SystemTime::now())
    }

    pub fn quota_status_at_submit(&self, job: JobHandle) -> Option<QuotaStatus> {
        self.jobs.quota_status_at_submit(job)
    }

    fn execute_submit(
        &self,
        program: &QuantumProgram,
        opts: &RunOptions,
    ) -> Result<HardwareJob, HardwareError> {
        let circuit = PreparedCircuit::from_program(program)?;

        let cost = estimate_cost_seconds(opts);
        let quota_status = self.quota.try_reserve(cost, SystemTime::now())?;

        let remote_task_arn = self.client.submit(&circuit, opts)?;

        Ok(crate::registry::queued_job(
            remote_task_arn,
            quota_status,
            circuit.hash,
        ))
    }
}

crate::registry::impl_hardware_backend!(
    BraketBackend,
    supports_density_matrix = true,
    pending_message = "task has not completed yet -- call poll() until JobStatus::Completed",
    timeout_message = "run() timed out waiting for the Braket task to complete",
);
