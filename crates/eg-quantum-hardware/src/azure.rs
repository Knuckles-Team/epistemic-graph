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

use crate::{
    credentials::{CredentialSource, EnvCredentials},
    error::HardwareError,
    quota::{QuotaStatus, QuotaTracker, QuotaUnits},
    registry::{HardwareJob, HardwareJobRegistry, PreparedCircuit},
    transport::{Body, HttpRequest, HttpTransport, Method, ProviderHttp, ReqwestTransport},
};
use eg_quantum_core::backend::{BackendId, JobHandle, JobStatus, RunOptions};
use eg_quantum_core::ir::QuantumProgram;
use eg_quantum_core::result::QuantumResult;
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

fn estimate_cost_units(opts: &RunOptions) -> QuotaUnits {
    QuotaUnits(opts.shots.unwrap_or(1).max(1))
}

/// Azure's operator-declared budget and its usage lifecycle.
///
/// Azure is the only provider in this crate whose free surface has no fixed
/// quota. Keeping configuration, one-time initialization, reservation, and
/// snapshots together prevents the backend's request lifecycle from owning
/// budget policy as an incidental detail.
struct AzureQuota {
    tracker: Mutex<Option<QuotaTracker>>,
    initialized: OnceLock<()>,
}

impl Default for AzureQuota {
    fn default() -> Self {
        AzureQuota {
            tracker: Mutex::new(None),
            initialized: OnceLock::new(),
        }
    }
}

impl AzureQuota {
    fn ensure_configured<C: CredentialSource>(&self, credentials: &C) -> Result<(), HardwareError> {
        if self.initialized.get().is_some() {
            return Ok(());
        }
        let units = credentials
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
        let window_days = credentials
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

    fn reserve(&self, cost: QuotaUnits) -> Result<QuotaStatus, HardwareError> {
        let guard = self.tracker.lock().expect("quota mutex poisoned");
        let tracker = guard
            .as_ref()
            .expect("AzureQuota::ensure_configured must run before reserve");
        Ok(tracker.try_reserve(cost, SystemTime::now())?)
    }

    fn status(&self) -> Option<QuotaStatus> {
        self.tracker
            .lock()
            .expect("quota mutex poisoned")
            .as_ref()
            .map(|q| q.status(SystemTime::now()))
    }
}

/// Azure-specific request/authentication client.
///
/// Keeping workspace URL construction, AAD exchange, and job-wire translation in
/// this object makes the backend a quota/registry owner instead of a second HTTP
/// client. The generic credentials and transport remain injectable for tests.
struct AzureClient<C: CredentialSource, T: HttpTransport> {
    credentials: C,
    transport: T,
}

impl<C: CredentialSource, T: HttpTransport> AzureClient<C, T> {
    fn new(credentials: C, transport: T) -> Self {
        AzureClient {
            credentials,
            transport,
        }
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
        ProviderHttp::new(&self.transport, "azure-quantum").oauth_token(req, "AAD")
    }

    fn auth_header(&self) -> Result<(String, String), HardwareError> {
        Ok((
            "Authorization".to_string(),
            format!("Bearer {}", self.aad_token()?),
        ))
    }

    fn submit(
        &self,
        circuit: &PreparedCircuit,
        opts: &RunOptions,
    ) -> Result<String, HardwareError> {
        let api_base = self.api_base()?;
        let headers = vec![
            self.auth_header()?,
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
                "circuitHash": circuit.hash.to_hex(),
                "nQubits": circuit.n_qubits,
            },
        });
        let req = HttpRequest {
            method: Method::Post,
            url: format!("{api_base}/jobs"),
            headers,
            body: Some(Body::Json(envelope)),
        };
        let response =
            ProviderHttp::new(&self.transport, "azure-quantum").send_checked(req, &[200, 201])?;
        crate::registry::remote_id(&response.body, "id", "job response missing id")
    }

    fn job_request(&self, suffix: &str) -> Result<crate::transport::HttpResponse, HardwareError> {
        let api_base = self.api_base()?;
        let req = HttpRequest {
            method: Method::Get,
            url: format!("{api_base}/jobs/{suffix}"),
            headers: vec![self.auth_header()?],
            body: None,
        };
        ProviderHttp::new(&self.transport, "azure-quantum").send_checked(req, &[200])
    }

    fn refresh_status(&self, record: &mut HardwareJob) -> Result<(), HardwareError> {
        record.refresh_with(|record| {
            let response = self.job_request(&record.remote_id)?;
            let status = response
                .body
                .get("status")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            Ok(match status {
                "Waiting" => JobStatus::Queued,
                "Executing" => JobStatus::Running,
                "Succeeded" => JobStatus::Completed,
                "Cancelled" => JobStatus::Cancelled,
                "Failed" => JobStatus::Failed("Azure Quantum job reported Failed".to_string()),
                other => JobStatus::Failed(format!("unrecognized Azure job status {other:?}")),
            })
        })
    }

    fn fetch_result(
        &self,
        backend_id: &BackendId,
        record: &mut HardwareJob,
    ) -> Result<QuantumResult, HardwareError> {
        record.resolve_result(|record| {
            let suffix = format!("{}/results", record.remote_id);
            let response = self.job_request(&suffix)?;
            Ok(crate::registry::hardware_result(
                backend_id,
                record,
                &response.body,
                "counts",
                None,
            ))
        })
    }

    fn cancel(&self, record: &HardwareJob) -> Result<(), HardwareError> {
        let api_base = self.api_base()?;
        let req = HttpRequest {
            method: Method::Delete,
            url: format!("{api_base}/jobs/{}", record.remote_id),
            headers: vec![self.auth_header()?],
            body: None,
        };
        ProviderHttp::new(&self.transport, "azure-quantum")
            .send(req)
            .map(|_| ())
    }
}

pub struct AzureQuantumBackend<
    C: CredentialSource = EnvCredentials,
    T: HttpTransport = ReqwestTransport,
> {
    id: BackendId,
    client: AzureClient<C, T>,
    /// Owns the mandatory operator-declared budget and its usage state; see
    /// [`AzureQuota`] for why this is separate from the request lifecycle.
    quota: AzureQuota,
    jobs: HardwareJobRegistry,
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
            client: AzureClient::new(credentials, transport),
            quota: AzureQuota::default(),
            jobs: HardwareJobRegistry::default(),
        }
    }

    pub fn quota_status(&self) -> Option<QuotaStatus> {
        self.quota.status()
    }

    pub fn quota_status_at_submit(&self, job: JobHandle) -> Option<QuotaStatus> {
        self.jobs.quota_status_at_submit(job)
    }

    fn execute_submit(
        &self,
        program: &QuantumProgram,
        opts: &RunOptions,
    ) -> Result<HardwareJob, HardwareError> {
        self.quota.ensure_configured(&self.client.credentials)?;
        let circuit = PreparedCircuit::from_program(program)?;

        let quota_status = self.quota.reserve(estimate_cost_units(opts))?;
        let remote_job_id = self.client.submit(&circuit, opts)?;

        Ok(crate::registry::queued_job(
            remote_job_id,
            quota_status,
            circuit.hash,
        ))
    }
}

crate::registry::impl_hardware_backend!(
    AzureQuantumBackend,
    supports_density_matrix = false,
    pending_message = "job has not completed yet -- call poll() until JobStatus::Completed",
    timeout_message = "run() timed out waiting for the Azure Quantum job to complete",
);
