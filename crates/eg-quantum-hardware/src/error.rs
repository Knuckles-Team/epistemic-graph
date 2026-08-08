//! Errors internal to this crate's adapters, mapped to
//! [`eg_quantum_core::backend::BackendError`] at each backend's trait boundary (the
//! same two-layer error shape `eg-quantum-sim`'s `SimError` uses).

use crate::quota::QuotaExceeded;
use eg_quantum_core::backend::{BackendError, BackendId};

#[derive(Debug, thiserror::Error)]
pub enum HardwareError {
    #[error("missing credential: {0}")]
    Credential(#[from] crate::credentials::CredentialError),

    #[error(transparent)]
    Quota(#[from] QuotaExceeded),

    #[error("transport error calling {provider}: {source}")]
    Transport {
        provider: &'static str,
        #[source]
        source: crate::transport::TransportError,
    },

    #[error("{provider} rejected the request (HTTP {status}): {body}")]
    ProviderRejected {
        provider: &'static str,
        status: u16,
        body: String,
    },

    #[error("response did not match the expected shape: {0}")]
    UnexpectedResponse(String, #[source] Option<serde_json::Error>),

    #[error("unknown job handle")]
    UnknownJob,

    #[error("job {0} has not reached a terminal state yet")]
    NotTerminal(u64),

    #[error("job {0} failed on the provider side: {1}")]
    RemoteFailed(u64, String),

    #[error("circuit rejected: {0}")]
    InvalidProgram(String),

    /// A provider with no fixed free-tier quota (Azure Quantum) refusing to submit
    /// until an operator explicitly declares a budget -- see `azure.rs` module docs.
    /// Deliberately its own variant (not reusing `Quota`, which carries a
    /// `QuotaExceeded` built from a REAL tracker) because there is no tracker to
    /// build one from yet; maps to the same `BackendError::ResourceLimit` a real
    /// exhaustion would, since "no budget configured" and "budget exhausted" are the
    /// same refusal from a caller's perspective -- zero usable budget either way.
    #[error("{0}")]
    BudgetNotConfigured(String),
}

impl HardwareError {
    /// Map into the shared, vendor-neutral [`BackendError`] every `QuantumBackend`
    /// method must return. Quota exhaustion and unbound encoding both map to
    /// `ResourceLimit`/`InvalidProgram` respectively so a caller reasoning generically
    /// over `BackendError` (the planner's R0 hard-constraint elimination, a future
    /// Q8 control-plane surface) does not need to know this crate exists.
    pub fn into_backend_error(self, backend_id: &BackendId) -> BackendError {
        match self {
            HardwareError::Quota(q) => BackendError::ResourceLimit(q.to_string()),
            HardwareError::BudgetNotConfigured(msg) => BackendError::ResourceLimit(msg),
            HardwareError::InvalidProgram(msg) => BackendError::InvalidProgram(msg),
            HardwareError::UnknownJob => BackendError::UnknownJob,
            HardwareError::NotTerminal(_) => BackendError::Unsupported(backend_id.clone()),
            HardwareError::RemoteFailed(_, msg) => BackendError::Execution(msg),
            other => BackendError::Execution(other.to_string()),
        }
    }
}
