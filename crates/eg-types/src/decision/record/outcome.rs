//! What a decision concluded: a proved assembly, or a typed abstention.
//!
//! There is no third arm. An engine that cannot prove an assembly says so and
//! names what it could not resolve; it never returns a guess wearing the shape
//! of an answer.

use serde::{Deserialize, Serialize};

use super::premise::Violation;
use crate::agent_component::ComponentDependency;
use crate::contract::BoundedVec;
use crate::solve::{Certificate, ObjectiveValue, Scalar};

/// One slot of the assembled graph and the component bound to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SlotAssignment {
    pub slot: String,
    pub component: ComponentDependency,
}

/// Why the engine declined to answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AbstainReason {
    /// A required capability no candidate covers.
    UncoveredCapability { iri: String },
    /// A declared capability IRI that is not a native term.
    UnresolvedCapabilityIri { iri: String },
    /// Free text no mapping resolved to a task.
    UnmappedTask { text_digest: String },
    /// The constraints named cannot be satisfied together.
    Infeasible { constraints: BoundedVec<String, 64> },
    /// The search budget ran out above the accepted gap.
    BudgetExhausted {
        #[serde(default)]
        incumbent: Option<String>,
        lower_bound: Scalar,
    },
    /// A fact the objective needs is absent on a candidate.
    UnknownFact { component_id: String, field: String },
    /// An external agent without its required observation.
    IneligibleExternalAgent { component_id: String },
    /// The record itself would exceed the durable record bound.
    RecordTooLarge { bytes: u64 },
    /// The request tried to widen the pinned policy.
    PolicyLoosening { field: String },
    /// Statistical: below the policy's support, coverage or risk floor. It
    /// names no invisible data, by construction.
    InsufficientConfidence,
}

/// Why one candidate did not win its slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WhyNot {
    pub component_id: String,
    pub slot: String,
    /// The objective it would have forced, when it was feasible but worse.
    #[serde(default)]
    pub forced_objective: Option<ObjectiveValue>,
    /// The rule that removed it, when it was not feasible at all.
    #[serde(default)]
    pub violation: Option<Violation>,
}

/// The conclusion of one assembly decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DecisionOutcome {
    /// An assembly, with the certificate that proves it optimal.
    Solved {
        graph_digest: String,
        slots: BoundedVec<SlotAssignment, 64>,
        certificate: Certificate,
    },
    /// No assembly, and exactly why.
    Abstained {
        reasons: BoundedVec<AbstainReason, 64>,
    },
}
