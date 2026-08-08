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
use crate::sigv4::{self, AwsCredentials};
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
use std::sync::Mutex;
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

struct JobRecord {
    remote_task_arn: String,
    status: JobStatus,
    result: Option<QuantumResult>,
    quota_at_submit: QuotaStatus,
    circuit_hash: CircuitHash,
}

fn estimate_cost_seconds(opts: &RunOptions) -> QuotaUnits {
    // Same conservative per-shot-second proxy as `ibm.rs` -- see that module's
    // `estimate_cost_seconds` docs.
    QuotaUnits(opts.shots.unwrap_or(1).max(1))
}

pub struct BraketBackend<C: CredentialSource = EnvCredentials, T: HttpTransport = ReqwestTransport>
{
    id: BackendId,
    credentials: C,
    transport: T,
    region: String,
    quota: QuotaTracker,
    jobs: Mutex<HashMap<u64, JobRecord>>,
    next_handle: AtomicU64,
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
            credentials,
            transport,
            region: DEFAULT_REGION.to_string(),
            quota,
            jobs: Mutex::new(HashMap::new()),
            next_handle: AtomicU64::new(0),
        }
    }

    pub fn quota_status(&self) -> QuotaStatus {
        self.quota.status(SystemTime::now())
    }

    pub fn quota_status_at_submit(&self, job: JobHandle) -> Option<QuotaStatus> {
        self.jobs
            .lock()
            .expect("job store mutex poisoned")
            .get(&job.0)
            .map(|r| r.quota_at_submit)
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

        // NOT the real CreateQuantumTask payload (requires an OpenQASM3 `action`
        // document -- same Q2 dependency noted in `ibm.rs` and the crate root docs).
        // This envelope carries the circuit's identity only.
        let envelope = serde_json::json!({
            "deviceArn": "arn:aws:braket:::device/quantum-simulator/amazon/sv1",
            "shots": opts.shots.unwrap_or(1),
            "clientToken": circuit_hash.to_hex(),
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
        let resp = self
            .transport
            .send(req)
            .map_err(|e| HardwareError::Transport {
                provider: "aws-braket",
                source: e,
            })?;
        if resp.status != 200 && resp.status != 201 {
            return Err(HardwareError::ProviderRejected {
                provider: "aws-braket",
                status: resp.status,
                body: resp.body.to_string(),
            });
        }
        let remote_task_arn = resp
            .body
            .get("quantumTaskArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                HardwareError::UnexpectedResponse(
                    "CreateQuantumTask response missing quantumTaskArn".to_string(),
                    None,
                )
            })?
            .to_string();

        Ok(JobRecord {
            remote_task_arn,
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
        let path = format!("/quantum-task/{}", record.remote_task_arn);
        let headers = self.signed_headers("GET", &path, b"")?;
        let req = HttpRequest {
            method: Method::Get,
            url: format!("https://{}{}", self.host(), path),
            headers,
            body: None,
        };
        let resp = self
            .transport
            .send(req)
            .map_err(|e| HardwareError::Transport {
                provider: "aws-braket",
                source: e,
            })?;
        if resp.status != 200 {
            return Err(HardwareError::ProviderRejected {
                provider: "aws-braket",
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
            "CREATED" | "QUEUED" => JobStatus::Queued,
            "RUNNING" => JobStatus::Running,
            "COMPLETED" => JobStatus::Completed,
            "CANCELLED" => JobStatus::Cancelled,
            "FAILED" => JobStatus::Failed("Braket task reported FAILED".to_string()),
            other => JobStatus::Failed(format!("unrecognized Braket task status {other:?}")),
        };
        Ok(())
    }

    fn fetch_result(&self, record: &mut JobRecord) -> Result<QuantumResult, HardwareError> {
        if let Some(r) = &record.result {
            return Ok(r.clone());
        }
        let path = format!("/quantum-task/{}/result", record.remote_task_arn);
        let headers = self.signed_headers("GET", &path, b"")?;
        let req = HttpRequest {
            method: Method::Get,
            url: format!("https://{}{}", self.host(), path),
            headers,
            body: None,
        };
        let resp = self
            .transport
            .send(req)
            .map_err(|e| HardwareError::Transport {
                provider: "aws-braket",
                source: e,
            })?;
        if resp.status != 200 {
            return Err(HardwareError::ProviderRejected {
                provider: "aws-braket",
                status: resp.status,
                body: resp.body.to_string(),
            });
        }
        let counts: std::collections::BTreeMap<String, u64> = resp
            .body
            .get("measurementCounts")
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

impl<C: CredentialSource, T: HttpTransport> QuantumBackend for BraketBackend<C, T> {
    fn backend_id(&self) -> BackendId {
        self.id.clone()
    }

    fn family(&self) -> BackendFamily {
        BackendFamily::Hardware
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            supports_density_matrix: true, // Braket offers DM1
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
                "task has not completed yet -- call poll() until JobStatus::Completed".to_string(),
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
        let path = format!("/quantum-task/{}", record.remote_task_arn);
        let headers = self
            .signed_headers("PUT", &format!("{path}/cancel"), b"")
            .map_err(|e| e.into_backend_error(&self.id))?;
        let req = HttpRequest {
            method: Method::Put, // Braket's real CancelQuantumTask is PUT .../cancel
            url: format!("https://{}{}/cancel", self.host(), path),
            headers,
            body: None,
        };
        self.transport.send(req).map_err(|e| {
            HardwareError::Transport {
                provider: "aws-braket",
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
                            "run() timed out waiting for the Braket task to complete".to_string(),
                        ));
                    }
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(Duration::from_secs(10));
                }
            }
        }
    }
}
