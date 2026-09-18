//! The decision policy: what "better" means, and when the engine must abstain.
//!
//! A policy is data, not code. It is either the engine default or the body of
//! a published `DecisionPolicy` component, and it is pinned by digest inside
//! every record it decided, so a record can be re-checked against the exact
//! rules that produced it.

use serde::{Deserialize, Serialize};

use super::numeric::{QuantisedValue, UnitRationalWire};
use super::DecisionErrorCode;
use crate::contract::BoundedVec;
use crate::solve::Scalar;

/// The engine default node budget when a policy does not name one.
pub const DEFAULT_DECISION_NODE_BUDGET: u64 = 100_000;

/// One level of the objective, named rather than positional so a record says
/// which quantity a budget was exhausted on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ObjectiveLevelKind {
    /// How many required capabilities stay uncovered.
    Uncovered,
    /// How many components the assembly selects.
    Components,
    /// Declared cost of the selection, in the budget's currency.
    DeclaredCost,
    /// Declared p95 latency of the selection.
    DeclaredP95Latency,
}

/// One weighted objective level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WeightedLevel {
    pub level: ObjectiveLevelKind,
    pub weight: u64,
}

/// How the objective levels combine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "order", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ObjectiveOrder {
    /// Strict priority: any change in an earlier level outweighs every
    /// possible change in all later ones together.
    Lexicographic {
        levels: BoundedVec<ObjectiveLevelKind, 8>,
    },
    /// One weighted sum.
    Weighted {
        weights: BoundedVec<WeightedLevel, 8>,
    },
}

/// What an unknown cost means when a budget is in force.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum UnknownCostRule {
    /// A strict budget excludes every candidate whose cost is unknown.
    ExcludeWhenStrict,
    /// An unknown cost ranks below every known one, and is never read as zero.
    RankBelowKnown,
}

/// How much exploration a cold, uncalibrated question may spend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ExplorationBudget {
    pub fraction: UnitRationalWire,
    pub spend_at_risk_micros: u64,
    /// Question ids exploration is permitted for. Empty forbids it everywhere.
    pub questions: BoundedVec<String, 16>,
}

/// What the engine does before a head is calibrated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ColdStart {
    /// Only the deterministic constraint ladder runs.
    DeterministicOnly,
    /// Scores are produced and labelled uncalibrated; nothing acts on them.
    AdvisoryUncalibrated,
    /// Bounded exploration is allowed within `budget`.
    Explore { budget: ExplorationBudget },
}

/// How much of a run's trace has to survive for its outcome to count as a
/// label. Mirrors the record's own [`super::TraceFidelity`] at policy
/// granularity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum TraceFidelityLevel {
    FullStep,
    ToolCalls,
    FinalOutput,
}

/// The statistical half of a policy: the risk, coverage and support levels
/// below which the engine abstains instead of acting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct StatisticalPolicy {
    /// Miscoverage level of the prediction set.
    pub alpha: UnitRationalWire,
    /// Risk level the controlled procedure must hold.
    pub epsilon: UnitRationalWire,
    /// Failure probability of that guarantee.
    pub delta: UnitRationalWire,
    /// Fewest records before any calibrated claim is made.
    pub n_min: u64,
    /// Fewest supporting records per option.
    pub min_support: u64,
    /// Smallest effective sample size an off-policy estimate may report.
    pub min_ess: QuantisedValue,
    pub min_outcome_fidelity: TraceFidelityLevel,
    /// Whether tenant-public features may be read for this question.
    pub tenant_public_features: bool,
    /// Share of decisions sampled into the audit stream.
    pub audit_sample: UnitRationalWire,
}

/// The complete decision policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionPolicy {
    pub schema_version: u16,
    pub objective: ObjectiveOrder,
    pub unknown_cost: UnknownCostRule,
    /// Largest objective gap accepted instead of reporting budget exhaustion.
    pub accepted_gap: Scalar,
    pub node_budget: u64,
    pub max_templates: u8,
    pub max_slots: u8,
    pub max_nogood_rounds: u8,
    pub max_why_not_per_slot: u8,
    /// Whether an A2A agent card needs an observation before it is selectable.
    pub a2a_requires_observation: bool,
    pub cold_start: ColdStart,
    #[serde(default)]
    pub statistical: Option<StatisticalPolicy>,
}

impl DecisionPolicy {
    /// The validating constructor: a decoded policy is usable only after this
    /// returns it. A newer schema version is a typed refusal, not a mismatch
    /// discovered later against an unfamiliar digest.
    pub fn checked(self) -> Result<Self, DecisionErrorCode> {
        if self.schema_version != super::DECISION_POLICY_SCHEMA_VERSION {
            return Err(DecisionErrorCode::DecisionRecordVersionUnsupported);
        }
        if self.accepted_gap.get() < 0 || self.node_budget == 0 {
            return Err(DecisionErrorCode::PolicyLoosening);
        }
        if self.max_templates == 0 || self.max_slots == 0 {
            return Err(DecisionErrorCode::PolicyLoosening);
        }
        Ok(self)
    }
}
