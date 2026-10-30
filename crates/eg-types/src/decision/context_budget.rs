//! Context window budget and recursive parallel-map limits
//! (EG-DECISION-ENGINE-R137.1 and R137.2), and the certified
//! multi-resolution context-package selection records
//! (EG-DECISION-ENGINE-R137.7).
//!
//! A prompt that passes the over-window fraction of the model window is split
//! in halves, depth by depth, until every chunk fits under that fraction. Each
//! leaf chunk is one model call and each pairwise merge is one more, so a plan
//! of `c` chunks costs `2c - 1` model calls and reads `c * chunk_tokens` tokens.
//! The engine plans only; it runs no model.
//!
//! The package records type the candidates, the budget ceilings and the
//! certified selection of the bounded 0-1 package solver. They are wire
//! records only: solving, capacity arithmetic and the certificate belong to
//! the solver decision.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::numeric::UnitRationalWire;
use crate::contract::BoundedVec;

/// Named, replaceable defaults (EG-DECISION-ENGINE-R137.2).
pub const DEFAULT_OVER_WINDOW_NUMERATOR: u64 = 4;
pub const DEFAULT_OVER_WINDOW_DENOMINATOR: u64 = 5;
pub const DEFAULT_MAX_ITEMS: u32 = 1000;
pub const DEFAULT_MAX_DEPTH: u8 = 3;
pub const DEFAULT_MAX_MODEL_CALLS: u32 = 1000;

/// Bounds on recursive parallel work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ParallelMapLimits {
    pub max_depth: u8,
    pub max_items: u32,
    /// Ceiling on the tokens all leaf calls read together.
    pub max_cost: u64,
    pub max_model_calls: u32,
}

impl Default for ParallelMapLimits {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_MAX_DEPTH,
            max_items: DEFAULT_MAX_ITEMS,
            max_cost: u64::MAX,
            max_model_calls: DEFAULT_MAX_MODEL_CALLS,
        }
    }
}

/// The model window and the share of it one prompt may fill.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WindowBudget {
    pub window_tokens: u64,
    pub over_window_fraction: UnitRationalWire,
}

impl WindowBudget {
    /// A window with the default over-window fraction of 0.8.
    pub fn with_default_fraction(window_tokens: u64) -> Self {
        Self {
            window_tokens,
            over_window_fraction: UnitRationalWire::new(
                DEFAULT_OVER_WINDOW_NUMERATOR,
                DEFAULT_OVER_WINDOW_DENOMINATOR,
            )
            .expect("the default fraction is a unit rational"),
        }
    }

    /// The largest prompt that does not exceed the window share.
    pub fn threshold_tokens(&self) -> u64 {
        let fraction = self.over_window_fraction;
        let scaled = u128::from(self.window_tokens) * u128::from(fraction.numerator())
            / u128::from(fraction.denominator());
        u64::try_from(scaled).expect("a unit fraction never grows the window")
    }
}

/// The derived plan. `depth` 0 and one chunk means the prompt fits as is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OverWindowPlan {
    pub exceeds: bool,
    pub chunks: u32,
    pub depth: u8,
    pub chunk_tokens: u64,
    pub model_calls: u32,
}

/// Which limit stopped the plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ContextBudgetLimit {
    MaxDepth,
    MaxCost,
    MaxModelCalls,
    WindowTokens,
}

/// A typed refusal; it carries no plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ContextBudgetRefusal {
    BudgetExhausted {
        limit: ContextBudgetLimit,
        observed: u64,
    },
    TooManyItems {
        max_items: u32,
        observed: u64,
    },
}

fn leaf(prompt_tokens: u64, depth: u8) -> (u64, u64) {
    let chunks = 1u64 << depth;
    (chunks, prompt_tokens.div_ceil(chunks))
}

fn check_limits(
    chunks: u64,
    chunk_tokens: u64,
    limits: &ParallelMapLimits,
) -> Result<(), ContextBudgetRefusal> {
    if chunks > u64::from(limits.max_items) {
        return Err(ContextBudgetRefusal::TooManyItems {
            max_items: limits.max_items,
            observed: chunks,
        });
    }
    let calls = 2 * chunks - 1;
    if calls > u64::from(limits.max_model_calls) {
        return Err(ContextBudgetRefusal::BudgetExhausted {
            limit: ContextBudgetLimit::MaxModelCalls,
            observed: calls,
        });
    }
    let cost = chunks.saturating_mul(chunk_tokens);
    if cost > limits.max_cost {
        return Err(ContextBudgetRefusal::BudgetExhausted {
            limit: ContextBudgetLimit::MaxCost,
            observed: cost,
        });
    }
    Ok(())
}

/// Decide whether a prompt exceeds its window share and, if so, the shallowest
/// halving split whose chunks each fit within every limit.
pub fn decide_over_window(
    prompt_tokens: u64,
    budget: &WindowBudget,
    limits: &ParallelMapLimits,
) -> Result<OverWindowPlan, ContextBudgetRefusal> {
    let threshold = budget.threshold_tokens();
    if threshold == 0 {
        return Err(ContextBudgetRefusal::BudgetExhausted {
            limit: ContextBudgetLimit::WindowTokens,
            observed: budget.window_tokens,
        });
    }
    if prompt_tokens <= threshold {
        return Ok(OverWindowPlan {
            exceeds: false,
            chunks: 1,
            depth: 0,
            chunk_tokens: prompt_tokens,
            model_calls: 1,
        });
    }
    // Past depth 31 the chunk count no longer fits the plan's u32 field.
    for depth in 1..=limits.max_depth.min(31) {
        let (chunks, chunk_tokens) = leaf(prompt_tokens, depth);
        check_limits(chunks, chunk_tokens, limits)?;
        if chunk_tokens <= threshold {
            return Ok(OverWindowPlan {
                exceeds: true,
                chunks: u32::try_from(chunks).expect("depth is at most 31"),
                depth,
                chunk_tokens,
                model_calls: u32::try_from(2 * chunks - 1).expect("bounded by max_model_calls"),
            });
        }
    }
    Err(ContextBudgetRefusal::BudgetExhausted {
        limit: ContextBudgetLimit::MaxDepth,
        observed: u64::from(limits.max_depth),
    })
}

/// The largest number of candidates one package selection may consider.
pub const MAX_CONTEXT_PACKAGE_CANDIDATES: usize = 64;

/// One cited range within a candidate package's source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackageCitedRange {
    /// The inclusive first offset of the cited range.
    pub start: u64,
    /// The exclusive one-past-the-end offset of the cited range.
    pub end: u64,
}

/// A monetary quantity: an exact minor-unit count in a named currency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackageMoney {
    pub currency: String,
    pub minor_units: u64,
}

/// One candidate context package: one resolution of one evidence unit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ContextPackageCandidate {
    pub candidate_id: String,
    /// The evidence unit this candidate is one resolution of.
    pub evidence_unit: String,
    /// The resolution this candidate provides. The pair
    /// `(evidence_unit, resolution)` is the resolution group: one candidate
    /// owns each group.
    pub resolution: String,
    pub source_id: String,
    pub cited_range: PackageCitedRange,
    pub source_revision: String,
    pub content_digest: String,
    /// The tokenizer identity the weight was measured under.
    pub tokenizer_id: String,
    pub tokenizer_version: String,
    /// The exact measured weight, in tokens.
    pub token_weight: u64,
    /// The candidate's net value, an exact rational in [0, 1].
    pub value: UnitRationalWire,
    /// Opaque ref naming how the weight and value were measured.
    pub measurement_provenance: String,
}

/// The budget a package selection is checked against. Each optional ceiling
/// carries its units in the field name; absence is the unknown fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ContextPackageBudget {
    pub window_tokens: u64,
    pub reserved_output_tokens: u64,
    /// An optional caller token ceiling; absent stays unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caller_token_ceiling: Option<u64>,
    /// The monetary ceiling; absent stays unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub monetary_ceiling: Option<PackageMoney>,
    /// The price per input token, in the ceiling's currency; absent stays
    /// unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_per_input_token: Option<PackageMoney>,
    /// The latency ceiling, in milliseconds; absent stays unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ceiling_ms: Option<u64>,
    /// The model's token throughput, tokens per second; absent stays unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_throughput_per_s: Option<u64>,
}

/// The certified selection a package request solved to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ContextPackageSelection {
    pub selected_candidate_ids: BoundedVec<String, MAX_CONTEXT_PACKAGE_CANDIDATES>,
    /// The capacity every active ceiling was checked against, in tokens.
    pub effective_capacity_tokens: u64,
    /// The total exact token weight of the selected packages.
    pub total_tokens: u64,
    /// The total monetary cost of the selected packages; absent when the
    /// price is unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_cost: Option<PackageMoney>,
    /// The total latency of the selected packages, in milliseconds.
    pub total_latency_ms: u64,
    /// The objective value on the declared fixed-point scale.
    pub objective: i64,
    pub input_digest: String,
    pub model_digest: String,
    pub certificate_digest: String,
    pub verified: bool,
}

/// A certified package-selection request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ContextPackageRequest {
    pub budget: ContextPackageBudget,
    pub candidates: BoundedVec<ContextPackageCandidate, MAX_CONTEXT_PACKAGE_CANDIDATES>,
    /// The per-token net-value floor a candidate must clear to be admitted.
    pub marginal_value_floor: UnitRationalWire,
    /// The deterministic work budget the solver may spend.
    pub solver_budget: u64,
}

/// Why a package record set was refused before solving.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PackageRecordRefusal {
    /// A candidate id appears more than once.
    DuplicateCandidateId { candidate_id: String },
    /// Two candidates claim the same resolution group.
    InconsistentResolutionGroup {
        candidate_id: String,
        evidence_unit: String,
        resolution: String,
    },
    /// A cited range is empty or inverted.
    MalformedRange { candidate_id: String },
    /// The total token weight overflows the token counter.
    WeightOverflow { observed: u128 },
}

impl ContextPackageRequest {
    /// Validate the record set before solving: duplicate ids, inconsistent
    /// resolution groups, malformed ranges and weight-sum overflow refuse.
    /// Measurements are unsigned, so a negative one is refused by the type
    /// itself on the way in.
    pub fn checked(self) -> Result<Self, PackageRecordRefusal> {
        let mut seen_ids = BTreeSet::new();
        let mut groups = BTreeSet::new();
        let mut total_weight: u128 = 0;
        for candidate in self.candidates.iter() {
            if !seen_ids.insert(candidate.candidate_id.as_str()) {
                return Err(PackageRecordRefusal::DuplicateCandidateId {
                    candidate_id: candidate.candidate_id.clone(),
                });
            }
            if !groups.insert((
                candidate.evidence_unit.as_str(),
                candidate.resolution.as_str(),
            )) {
                return Err(PackageRecordRefusal::InconsistentResolutionGroup {
                    candidate_id: candidate.candidate_id.clone(),
                    evidence_unit: candidate.evidence_unit.clone(),
                    resolution: candidate.resolution.clone(),
                });
            }
            if candidate.cited_range.end <= candidate.cited_range.start {
                return Err(PackageRecordRefusal::MalformedRange {
                    candidate_id: candidate.candidate_id.clone(),
                });
            }
            total_weight += u128::from(candidate.token_weight);
            if total_weight > u128::from(u64::MAX) {
                return Err(PackageRecordRefusal::WeightOverflow {
                    observed: total_weight,
                });
            }
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests;
