//! `DecisionRecord` v1: the durable, re-checkable account of one assembly.
//!
//! The record carries its inputs, not a reference to them, because the point
//! is that a reader can re-derive the conclusion without the library it was
//! decided against still holding the same revisions. `created_at_ms` is
//! recorded and never read by the math, so replaying a record on another host
//! at another time produces the same digest.

pub mod outcome;
pub mod premise;

use serde::{Deserialize, Serialize};

use super::errors::DecisionErrorCode;
use super::policy::DecisionPolicy;
use super::request::AssemblyRequest;
use crate::agent_component::{AgentComponentFacts, AgentComponentKind, ComponentDependency};
use crate::agent_library::AgentLibraryLifecycle;
use crate::contract::BoundedVec;
use crate::solve::Algorithm;

pub use outcome::{AbstainReason, DecisionOutcome, SlotAssignment, WhyNot};
pub use premise::{
    CoverageDerivation, DerivationEdge, EdgeSource, Elimination, PremiseClass, PremiseProvenance,
    PremiseRef, Violation,
};

/// The question a record answers. One variant in this wave; the tag exists so
/// a later question does not have to break the record shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DecisionQuestion {
    Assemble,
}

/// Where the candidates came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CandidateSourceRecord {
    /// One tenant-bound agent library snapshot.
    AgentLibrary {
        kinds: BoundedVec<AgentComponentKind, 16>,
        #[serde(default)]
        classification_under: Option<String>,
    },
    /// An RLS-filtered graph plan, pinned by its plan digest.
    Graph { plan_digest: String },
}

/// How the conclusion was reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ResolutionKind {
    Constraint,
    Entailment,
    Optimization,
    Statistical,
    Abstention,
}

/// The weakest class of evidence the conclusion rests on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum EvidenceClass {
    Proof,
    Observation,
    Claim,
}

/// How the derivation itself was produced. Only checkable derivations are
/// admitted in this wave, so the enum has one arm and a name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DerivationClass {
    Proof,
}

/// How complete the recorded trace is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "fidelity", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum TraceFidelity {
    /// Every step is present.
    FullStep,
    /// Steps were dropped to stay inside the record bound; each is named.
    Truncated { dropped: BoundedVec<String, 16> },
}

/// The solver identity a record was decided under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SolverIdentity {
    pub algorithm: Algorithm,
    pub node_budget: u64,
}

/// Everything known about one candidate at decision time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CandidateFacts {
    pub component_id: String,
    pub kind: AgentComponentKind,
    pub entry_revision: u64,
    pub definition_digest: String,
    pub lifecycle: AgentLibraryLifecycle,
    pub classification: BoundedVec<String, 64>,
    pub required_capabilities: BoundedVec<String, 64>,
    pub declared_capabilities: BoundedVec<String, 64>,
    pub requires: BoundedVec<ComponentDependency, 256>,
    pub facts: AgentComponentFacts,
    /// The premises the facts above rest on.
    pub fact_premises: BoundedVec<PremiseRef, 32>,
}

/// The complete input set, digested as `inputs_digest`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionInputs {
    pub request: AssemblyRequest,
    /// Sorted by `component_id`, so the digest does not depend on read order.
    pub candidates: BoundedVec<CandidateFacts, 64>,
    pub ontology_digest: String,
    pub policy: DecisionPolicy,
    pub policy_digest: String,
    pub catalog_digest: String,
    pub solver: SolverIdentity,
}

/// One assembly decision, in full.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionRecord {
    pub schema_version: u16,
    pub record_id: String,
    pub tenant_id: String,
    /// Privacy-safe persistence id of the caller, never a display name.
    pub caller_principal: String,
    /// Recorded, and never read by the math.
    pub created_at_ms: u64,
    pub question: DecisionQuestion,
    pub candidate_source: CandidateSourceRecord,
    pub inputs: DecisionInputs,
    pub inputs_digest: String,
    pub resolution_kind: ResolutionKind,
    pub evidence_class: EvidenceClass,
    pub derivation_class: DerivationClass,
    pub trace_fidelity: TraceFidelity,
    pub premises: BoundedVec<PremiseRef, 1024>,
    pub eliminated: BoundedVec<Elimination, 64>,
    pub derivations: BoundedVec<CoverageDerivation, 32>,
    pub outcome: DecisionOutcome,
    pub why_not: BoundedVec<WhyNot, 64>,
    pub record_digest: String,
}

impl DecisionRecord {
    /// The validating constructor. A decoded record becomes usable only after
    /// this returns it, so an unsupported version is a typed refusal instead
    /// of a digest mismatch nothing explains.
    pub fn checked(self) -> Result<Self, DecisionErrorCode> {
        if self.schema_version != super::DECISION_RECORD_SCHEMA_VERSION {
            return Err(DecisionErrorCode::DecisionRecordVersionUnsupported);
        }
        if !matches!(self.question, DecisionQuestion::Assemble) {
            return Err(DecisionErrorCode::DecisionReplayMismatch);
        }
        Ok(self)
    }

    /// Whether the outcome names an assembly.
    pub fn is_solved(&self) -> bool {
        matches!(self.outcome, DecisionOutcome::Solved { .. })
    }
}
