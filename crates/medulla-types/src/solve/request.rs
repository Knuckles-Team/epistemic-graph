//! The `Method::Solve` request and result.
//!
//! The server re-verifies the certificate it is about to return, so a reply
//! either carries a proof that checks or is refused with
//! [`SolveErrorCode::CertificateRejected`]: the caller never has to trust the
//! search that produced it.

use serde::{Deserialize, Serialize};

use super::certificate::{Certificate, SolverConfigSpec};
use super::scalar::Sha256Digest;
use super::spec::ModelSpec;
use crate::contract::closed_error_codes;

/// Format identity (RF-ADR-006) of [`SolveResult`].
pub const SOLVE_RESULT_SCHEMA_VERSION: u16 = 1;

/// One bounded 0-1 integer programme to solve.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SolveRequest {
    pub model: ModelSpec,
    /// Absent means the engine's default budget, leaf limit and denominator.
    #[serde(default)]
    pub config: Option<SolverConfigSpec>,
}

/// A solved model: the certificate plus the digest a caller pins it by.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SolveResult {
    pub schema_version: u16,
    pub certificate: Certificate,
    pub certificate_digest: Sha256Digest,
}

closed_error_codes! {
    /// Every typed refusal `Method::Solve` can answer with.
    pub enum SolveErrorCode {
        /// The model failed validation before any search started.
        ModelInvalid => "SOLVE_MODEL_INVALID",
        /// The supplied config is outside a named solver bound.
        ConfigInvalid => "SOLVE_CONFIG_INVALID",
        /// The engine's own re-verification of its certificate failed.
        CertificateRejected => "SOLVE_CERTIFICATE_REJECTED",
    }
}
