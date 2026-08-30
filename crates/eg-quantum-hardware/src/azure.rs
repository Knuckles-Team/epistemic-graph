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
    quota::QuotaStatus,
    registry::{self, BackendState, ConfiguredQuota, HardwareJob, PreparedCircuit},
    transport::{Body, HttpRequest, HttpTransport, Method, ProviderHttp, ReqwestTransport},
};
use eg_quantum_core::backend::{JobHandle, RunOptions};

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

const AZURE_STATUS_MAPPING: registry::StatusMapping = registry::StatusMapping {
    provider: "Azure",
    status_noun: "job",
    queued: &["Waiting"],
    running: &["Executing"],
    completed: &["Succeeded"],
    cancelled: &["Cancelled"],
    failed: &[("Failed", "Azure Quantum job reported Failed")],
};

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

    fn cancel_request(
        &self,
        record: &HardwareJob,
    ) -> Result<crate::transport::HttpResponse, HardwareError> {
        let api_base = self.api_base()?;
        let req = HttpRequest {
            method: Method::Delete,
            url: format!("{api_base}/jobs/{}", record.remote_id),
            headers: vec![self.auth_header()?],
            body: None,
        };
        ProviderHttp::new(&self.transport, "azure-quantum").send(req)
    }
}

impl<C: CredentialSource, T: HttpTransport> registry::ProviderClient for AzureClient<C, T> {
    fn request(
        &self,
        operation: registry::JobOperation,
        record: &HardwareJob,
    ) -> Result<crate::transport::HttpResponse, HardwareError> {
        match operation {
            registry::JobOperation::Cancel => self.cancel_request(record),
            _ => self.job_request(&operation.remote_suffix(&record.remote_id, "results")),
        }
    }
}

pub struct AzureQuantumBackend<
    C: CredentialSource = EnvCredentials,
    T: HttpTransport = ReqwestTransport,
> {
    state: BackendState,
    client: AzureClient<C, T>,
    /// Owns the mandatory operator-declared budget and its usage state; see
    /// [`ConfiguredQuota`] for why this is separate from the request lifecycle.
    quota: ConfiguredQuota,
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
            state: BackendState::new("hardware-azure"),
            client: AzureClient::new(credentials, transport),
            quota: ConfiguredQuota::default(),
        }
    }

    pub fn quota_status(&self) -> Option<QuotaStatus> {
        self.quota.status()
    }

    pub fn quota_status_at_submit(&self, job: JobHandle) -> Option<QuotaStatus> {
        self.state.quota_status_at_submit(job)
    }
}

impl<C: CredentialSource, T: HttpTransport> registry::ProviderSubmission
    for AzureQuantumBackend<C, T>
{
    fn before_submit(&self) -> Result<(), HardwareError> {
        self.quota.ensure_configured(
            &self.client.credentials,
            "Azure Quantum",
            AZURE_QUANTUM_BUDGET_UNITS_ENV,
            AZURE_QUANTUM_BUDGET_WINDOW_DAYS_ENV,
        )
    }

    fn reserve_submission(
        &self,
        cost: crate::quota::QuotaUnits,
    ) -> Result<QuotaStatus, HardwareError> {
        self.quota.reserve(cost)
    }

    fn submit_remote(
        &self,
        circuit: &PreparedCircuit,
        opts: &RunOptions,
    ) -> Result<String, HardwareError> {
        self.client.submit(circuit, opts)
    }
}

crate::registry::impl_hardware_backend!(
    AzureQuantumBackend,
    supports_density_matrix = false,
    pending_message = "job has not completed yet -- call poll() until JobStatus::Completed",
    timeout_message = "run() timed out waiting for the Azure Quantum job to complete",
    status_mapping = &AZURE_STATUS_MAPPING,
    result_mapping = registry::ResultMapping {
        counts_field: "counts",
        fidelity_field: None,
    },
);
