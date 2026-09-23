//! What a caller asks the assembly layer for, and what comes back.
//!
//! An assembly request states requirements and a candidate scope; it never
//! states an answer. Everything the engine used to reach its answer is echoed
//! into the record, so the reply is checkable without re-reading the library.

use serde::{Deserialize, Serialize};

use super::policy::DecisionPolicy;
use super::record::DecisionRecord;
use crate::agent_component::{
    AgentComponentCommittedResult, AgentComponentKind, ComponentDependency,
};
use crate::agent_graph::AgentGraphDraft;
use crate::agent_library::{AgentLibraryEntryDraft, AgentLibraryMutationContext};
use crate::contract::BoundedVec;
use crate::delegation::AgentGraphEntryRef;

/// Which policy decides this request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "policy", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DecisionPolicyRef {
    /// The engine default policy.
    Default,
    /// A published `DecisionPolicy` component, pinned by revision digest.
    Pinned { component: ComponentDependency },
}

/// How a free-text task was mapped onto native task IRIs, and by what.
///
/// A mapping is a CLAIM, never a proof: it is recorded with its producer so a
/// record that leans on one is classified by its weakest premise.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClaimProvenance {
    pub producer: String,
    #[serde(default)]
    pub model_profile: Option<ComponentDependency>,
    #[serde(default)]
    pub prompt_digest: Option<String>,
}

/// One claimed free-text-to-task mapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClaimedTaskMapping {
    /// `sha256:<hex>` of the free text, never the text.
    pub text_digest: String,
    pub task_iris: BoundedVec<String, 8>,
    pub provenance: ClaimProvenance,
}

/// A currency-denominated ceiling on the selection's declared cost.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CostBudget {
    /// ISO 4217, exactly three ASCII uppercase letters.
    pub currency: String,
    pub max_micros: u64,
    /// Strict means an unknown cost is excluded rather than ranked below.
    pub strict: bool,
}

/// Non-capability requirements the assembly must satisfy.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AssemblyConstraints {
    #[serde(default)]
    pub max_components: Option<u32>,
    #[serde(default)]
    pub context_budget_tokens: Option<u64>,
    #[serde(default)]
    pub cost_budget: Option<CostBudget>,
    #[serde(default)]
    pub max_p95_latency_ms: Option<u32>,
    #[serde(default)]
    pub require_tools: bool,
    #[serde(default)]
    pub require_structured_output: bool,
    #[serde(default)]
    pub modalities_in: BoundedVec<String, 16>,
    #[serde(default)]
    pub modalities_out: BoundedVec<String, 16>,
}

/// What the assembled agent has to be able to do.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AssemblyRequirements {
    /// Native `eg:task/*` IRIs.
    #[serde(default)]
    pub tasks: BoundedVec<String, 32>,
    /// Native `eg:capability/*` IRIs.
    #[serde(default)]
    pub capabilities: BoundedVec<String, 32>,
    #[serde(default)]
    pub task_mappings: BoundedVec<ClaimedTaskMapping, 32>,
    /// Digests of free text no mapping covered. Each becomes an abstention
    /// reason rather than a silently dropped requirement.
    #[serde(default)]
    pub unmapped_task_digests: BoundedVec<String, 32>,
    #[serde(default)]
    pub constraints: AssemblyConstraints,
    #[serde(default)]
    pub pins: BoundedVec<ComponentDependency, 64>,
    /// Component ids that must not be selected.
    #[serde(default)]
    pub denies: BoundedVec<String, 64>,
}

/// Which slice of the agent library the candidates come from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LibraryCandidateScope {
    pub kinds: BoundedVec<AgentComponentKind, 16>,
    /// Restrict to components classified under this ontology term.
    #[serde(default)]
    pub classification_under: Option<String>,
}

/// A caller's per-request solver budget. It may only TIGHTEN the policy; a
/// request that tries to widen one is refused with `POLICY_LOOSENING`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SolverBudget {
    pub node_budget: u64,
    pub max_why_not_per_slot: u8,
}

/// One assembly question.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AssemblyRequest {
    pub tenant_id: String,
    pub requirements: AssemblyRequirements,
    pub candidates: LibraryCandidateScope,
    /// Candidate graph templates. Empty asks for a one-agent graph.
    #[serde(default)]
    pub templates: BoundedVec<AgentGraphEntryRef, 8>,
    pub policy: DecisionPolicyRef,
    #[serde(default)]
    pub solver: Option<SolverBudget>,
}

/// The answer: always a record, and a graph draft only when one was proved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AssemblyResult {
    pub schema_version: u16,
    pub record: DecisionRecord,
    /// `Some` exactly when the record's outcome is `Solved`: a one-agent
    /// graph whose agent node pins [`Self::agent`]. Its `synthesis_evidence`
    /// is empty until the record is committed; the caller then sets it to the
    /// committed `DecisionRecord` component and publishes it (§4.6).
    #[serde(default)]
    pub graph: Option<AgentGraphDraft>,
    /// The assembled agents the graph runs, one per slot (one for the
    /// one-agent graph); empty exactly when `graph` is `None`. Each must be
    /// published before the graph that pins it.
    #[serde(default)]
    pub agents: BoundedVec<AgentLibraryEntryDraft, 8>,
    /// The exact integer programme the record's certificate was issued over,
    /// so a client can verify the certificate without trusting the engine.
    #[serde(default)]
    pub model: Option<crate::solve::ModelSpec>,
}

/// Commit one assembly record as a durable `DecisionRecord` component.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionCommitRequest {
    pub context: AgentLibraryMutationContext,
    /// This wave commits library-sourced v1 records only.
    pub record: DecisionRecord,
    /// Compare-and-set: the catalog digest the record was decided against.
    pub expected_catalog_digest: String,
}

/// The committed record's durable identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionCommitResult {
    pub schema_version: u16,
    /// `"decision:" + hex(record_digest)`.
    pub record_id: String,
    pub component: AgentComponentCommittedResult,
    /// True when an identical commit had already landed.
    pub replayed: bool,
}

/// Format identity (RF-ADR-006) of [`AssemblyResult`].
pub const ASSEMBLY_RESULT_SCHEMA_VERSION: u16 = 1;
/// Format identity (RF-ADR-006) of [`DecisionCommitResult`].
pub const DECISION_COMMIT_RESULT_SCHEMA_VERSION: u16 = 1;

/// The reserved component-id prefix every committed decision record carries.
pub const DECISION_COMPONENT_ID_PREFIX: &str = "decision:";

/// Extract the policy component a request pins, if any.
pub fn pinned_policy_component(policy: &DecisionPolicyRef) -> Option<&ComponentDependency> {
    match policy {
        DecisionPolicyRef::Default => None,
        DecisionPolicyRef::Pinned { component } => Some(component),
    }
}

/// Whether `candidate` only tightens `policy`'s solver budget.
pub fn solver_budget_tightens(policy: &DecisionPolicy, candidate: &SolverBudget) -> bool {
    loosened_solver_field(policy, candidate).is_none()
}

/// The first solver-budget field `candidate` would widen past `policy`, if any.
pub fn loosened_solver_field(
    policy: &DecisionPolicy,
    candidate: &SolverBudget,
) -> Option<&'static str> {
    if candidate.node_budget > policy.node_budget {
        return Some("node_budget");
    }
    if candidate.max_why_not_per_slot > policy.max_why_not_per_slot {
        return Some("max_why_not_per_slot");
    }
    None
}

/// The budget a request actually runs under: the policy's, tightened by the
/// request's own when it names one.
pub fn effective_solver_budget(policy: &DecisionPolicy, request: &AssemblyRequest) -> SolverBudget {
    request.solver.unwrap_or(SolverBudget {
        node_budget: policy.node_budget,
        max_why_not_per_slot: policy.max_why_not_per_slot,
    })
}
