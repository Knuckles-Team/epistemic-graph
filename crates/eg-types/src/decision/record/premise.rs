//! Premises, eliminations and coverage derivations: the checkable middle of a
//! decision record.
//!
//! Every fact the engine leaned on is named with the class of evidence it is
//! and where it came from, so a reader can classify a conclusion by its
//! WEAKEST premise rather than by the confidence of its conclusion.

use serde::{Deserialize, Serialize};

use super::super::policy::ObjectiveLevelKind;
use crate::contract::BoundedVec;

/// How strong one premise is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PremiseClass {
    /// True by the definition of the vocabulary.
    Definition,
    /// Asserted by a producer that is not the engine.
    Claim,
    /// Measured by a recorded evaluation.
    Observation,
    /// Derived by a checkable rule from other premises.
    Proof,
}

/// Where a premise came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "provenance", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PremiseProvenance {
    /// The engine's own ontology, at the digest named.
    NativeOntology { ontology_digest: String },
    /// The decision policy in force.
    Policy { policy_digest: String },
    /// A component publisher's declaration at an exact revision.
    Publisher {
        component_id: String,
        definition_digest: String,
    },
    /// A connector pack entry at an exact binding revision.
    ConnectorPack {
        connector: String,
        binding_revision: u64,
        pack_digest: String,
        entry_digest: String,
    },
    /// A free-text-to-task mapping asserted by a producer.
    ClaimedMapping {
        text_digest: String,
        producer: String,
    },
    /// A recorded evaluation.
    Observation { evaluation_id: String },
}

/// One fact the decision used.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PremiseRef {
    pub subject: String,
    pub fact: String,
    pub class: PremiseClass,
    pub provenance: PremiseProvenance,
}

/// Why a candidate was removed before the solver ran.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "violation", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum Violation {
    /// The request denied this component id.
    Denied,
    /// The component is withdrawn by its connector pack.
    Withdrawn,
    /// The component revision is retired.
    Retired,
    /// An external agent without the observation its policy requires.
    IneligibleExternalAgent,
    ContextWindowTooSmall {
        required: u64,
        available: u64,
    },
    MissingToolSupport,
    MissingStructuredOutput,
    MissingModality {
        iri: String,
    },
    /// Its cost is unknown and the budget is strict.
    UnknownCostUnderStrictBudget,
    OverBudget {
        level: ObjectiveLevelKind,
    },
    /// A template rule refused this component in this slot.
    TemplateValidation {
        code: String,
    },
}

/// One eliminated candidate and the rule that removed it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct Elimination {
    pub component_id: String,
    pub violation: Violation,
}

/// Which side of a subsumption edge the engine got it from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "edge", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum EdgeSource {
    /// The engine's own ontology.
    NativeOntology,
    /// A component's own classification list.
    ComponentClassification { component_id: String },
}

/// One `narrower ⊑ broader` step of a coverage chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DerivationEdge {
    pub narrower: String,
    pub broader: String,
    pub source: EdgeSource,
    pub class: PremiseClass,
}

/// How one required capability is covered, step by step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CoverageDerivation {
    pub required: String,
    /// The component that covers it, or `None` when nothing does.
    #[serde(default)]
    pub covered_by: Option<String>,
    pub chain: BoundedVec<DerivationEdge, 16>,
}
