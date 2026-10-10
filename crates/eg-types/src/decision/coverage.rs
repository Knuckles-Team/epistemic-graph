//! The capability-coverage query's wire contract (EG-DECISION-ENGINE-R126.2.1):
//! "which visible agent-library components and registered A2A agent cards
//! cover the capabilities this task needs?" A read-only answer computed by
//! [`super::derivation::capability_coverage_query`]; it ranks nothing and
//! commits nothing.

use serde::{Deserialize, Serialize};

use super::derivation::CapabilityCoverage;
use super::record::AbstainReason;
use super::request::{AssemblyRequirements, LibraryCandidateScope};

/// The schema version of [`CapabilityCoverageResult`].
pub const CAPABILITY_COVERAGE_RESULT_SCHEMA_VERSION: u16 = 1;

/// One coverage question over the verified tenant's own library.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CapabilityCoverageRequest {
    /// Must equal the verified request tenant: the tenant binding is the
    /// visibility boundary, exactly as for `AgentAssemble`.
    pub tenant_id: String,
    /// The native task (and/or capability) IRIs whose closure is covered.
    pub requirements: AssemblyRequirements,
    /// The Agent Library slice the covering components are drawn from.
    /// Registered A2A agent cards are always read in addition, as claims.
    pub candidates: LibraryCandidateScope,
}

/// The answer: either the per-capability coverage of the requirement
/// closure, or the reasons the closure could not be resolved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CapabilityCoverageOutcome {
    Covered {
        capabilities: Vec<CapabilityCoverage>,
    },
    Abstained {
        reasons: Vec<AbstainReason>,
    },
}

/// `Method::CapabilityCoverage`'s result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CapabilityCoverageResult {
    pub schema_version: u16,
    /// The digest of the native agent ontology the closure was derived under.
    pub ontology_digest: String,
    pub coverage: CapabilityCoverageOutcome,
}
