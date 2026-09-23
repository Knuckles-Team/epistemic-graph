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
use crate::agent_component::{
    AgentComponentEntry, AgentComponentFacts, AgentComponentKind, ComponentDependency,
};
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

impl CandidateFacts {
    /// The facts a decision reads from one published component revision.
    ///
    /// The ONE constructor both sides of a decision use: `AgentAssemble`
    /// builds its candidates with it and `DecisionCommit` rebuilds them from
    /// the pinned revisions and compares, so "the stored facts equal the
    /// published facts" is an equality of two values this function produced,
    /// not a field-by-field comparison that could miss a field.
    ///
    /// Every fact a component carries is its publisher's assertion, so every
    /// premise here is a CLAIM pinned to the exact revision that asserted it.
    pub fn from_entry(entry: &AgentComponentEntry) -> Result<Self, DecisionErrorCode> {
        let bounded = |_| DecisionErrorCode::RecordTooLarge;
        Ok(Self {
            component_id: entry.component_id.clone(),
            kind: entry.kind,
            entry_revision: entry.entry_revision,
            definition_digest: entry.definition_digest.clone(),
            lifecycle: entry.lifecycle,
            classification: BoundedVec::new(sorted(&entry.classification)).map_err(bounded)?,
            required_capabilities: BoundedVec::new(sorted(&entry.required_capabilities))
                .map_err(bounded)?,
            declared_capabilities: BoundedVec::new(sorted(&entry.declared_capabilities))
                .map_err(bounded)?,
            requires: BoundedVec::new(sorted(&entry.requires)).map_err(bounded)?,
            facts: entry.facts.clone(),
            fact_premises: BoundedVec::new(publisher_premises(entry)).map_err(bounded)?,
        })
    }

    /// Whether a declared classification term of this candidate is `iri` or
    /// is subsumed by it -- the relation coverage uses.
    pub fn classified_under(&self, iri: &str) -> bool {
        self.classification
            .iter()
            .any(|term| crate::agent_ontology::satisfies(term, iri))
    }
}

fn sorted<T: Clone + Ord>(values: &[T]) -> Vec<T> {
    let mut values = values.to_vec();
    values.sort();
    values.dedup();
    values
}

/// The claims a component revision makes, each pinned to that revision.
fn publisher_premises(entry: &AgentComponentEntry) -> Vec<PremiseRef> {
    let asserted = [
        ("classification", !entry.classification.is_empty()),
        (
            "required_capabilities",
            !entry.required_capabilities.is_empty(),
        ),
        ("facts", !matches!(entry.facts, AgentComponentFacts::Opaque)),
    ];
    asserted
        .into_iter()
        .filter(|(_, present)| *present)
        .map(|(fact, _)| PremiseRef {
            subject: entry.component_id.clone(),
            fact: fact.to_string(),
            class: PremiseClass::Claim,
            provenance: PremiseProvenance::Publisher {
                component_id: entry.component_id.clone(),
                definition_digest: entry.definition_digest.clone(),
            },
        })
        .collect()
}

/// One operator-published graph template an assembly may fill, pinned to the
/// exact revision it was read at. Its `Agent` nodes are the slots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TemplateFacts {
    pub graph_id: String,
    pub entry_revision: u64,
    pub definition_digest: String,
    pub shape: crate::agent_graph::AgentGraphShape,
}

impl TemplateFacts {
    /// The slot nodes: every `Agent` node, in shape order.
    pub fn slot_nodes(&self) -> Vec<&crate::agent_graph::AgentGraphNode> {
        self.shape
            .nodes
            .iter()
            .filter(|node| node.kind.agent_pin().is_some())
            .collect()
    }
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
    /// The graph templates the request named, read at their pinned revisions.
    /// Empty asks for a one-agent graph.
    #[serde(default)]
    pub templates: BoundedVec<TemplateFacts, 8>,
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
