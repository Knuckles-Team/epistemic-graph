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

/// The component attribute a published `DecisionPolicy` carries its body in:
/// the policy's canonical JSON. The component's `content_digest` must be the
/// policy's digest, so a pinned policy is exactly the body that was reviewed.
pub const DECISION_POLICY_ATTRIBUTE: &str = "decision.policy";

/// The policy a `DecisionPolicy` component's attributes carry, verified against
/// its content digest and its own validating constructor.
pub fn policy_from_attributes(
    attributes: &std::collections::BTreeMap<String, String>,
    content_digest: &str,
) -> Result<DecisionPolicy, DecisionErrorCode> {
    let body = attributes
        .get(DECISION_POLICY_ATTRIBUTE)
        .ok_or(DecisionErrorCode::PolicyBodyUnavailable)?;
    let policy: DecisionPolicy =
        serde_json::from_str(body).map_err(|_| DecisionErrorCode::PolicyBodyUnavailable)?;
    if super::digest::policy_digest(&policy) != content_digest {
        return Err(DecisionErrorCode::PolicyBodyUnavailable);
    }
    policy.checked()
}

/// How many excluded options per slot the engine explains by default.
pub const DEFAULT_WHY_NOT_PER_SLOT: u8 = 3;

impl DecisionPolicy {
    /// The engine default: the lexicographic order of DECIDE-LAYER-DESIGN §7.3
    /// (uncovered, then fewest components, then declared cost, then declared
    /// p95 latency), an unknown cost excluded under a strict budget, no
    /// accepted gap, the default node budget and bounded explanations.
    pub fn engine_default() -> Self {
        Self {
            schema_version: super::DECISION_POLICY_SCHEMA_VERSION,
            objective: ObjectiveOrder::Lexicographic {
                levels: BoundedVec::new(vec![
                    ObjectiveLevelKind::Uncovered,
                    ObjectiveLevelKind::Components,
                    ObjectiveLevelKind::DeclaredCost,
                    ObjectiveLevelKind::DeclaredP95Latency,
                ])
                .expect("four levels fit the eight-level bound"),
            },
            unknown_cost: UnknownCostRule::ExcludeWhenStrict,
            accepted_gap: Scalar::new(0),
            node_budget: DEFAULT_DECISION_NODE_BUDGET,
            max_templates: super::MAX_ASSEMBLY_TEMPLATES as u8,
            max_slots: super::MAX_TEMPLATE_SLOTS as u8,
            max_nogood_rounds: 8,
            max_why_not_per_slot: DEFAULT_WHY_NOT_PER_SLOT,
            a2a_requires_observation: true,
            cold_start: ColdStart::DeterministicOnly,
            statistical: None,
        }
    }

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
