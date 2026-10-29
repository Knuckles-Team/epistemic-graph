//! Evolution round verdicts and per-stage model allocation types
//! (MEDULLA-DECISION-ENGINE-R136.1 and R136.2).
//!
//! A round declares which tasks it expects to fix and which it expects to
//! regress. The verdict compares the observed flips with that declaration.

use serde::{Deserialize, Serialize};

use super::numeric::UnitRationalWire;

/// The named default round count.
pub const DEFAULT_EVOLUTION_ROUNDS: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PredictionDeclaration {
    pub round: u32,
    /// Tasks predicted to flip from fail to pass.
    pub fixes: Vec<String>,
    /// Tasks predicted to flip from pass to fail.
    pub regressions: Vec<String>,
}

/// One task's result before and after the round.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RoundTaskOutcome {
    pub task_id: String,
    pub passed_before: bool,
    pub passed_after: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum RoundVerdict {
    Verified,
    Falsified,
    Unverified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EvolutionDefaults {
    pub rounds: u32,
    pub flip_fraction: UnitRationalWire,
}

impl Default for EvolutionDefaults {
    fn default() -> Self {
        Self {
            rounds: DEFAULT_EVOLUTION_ROUNDS,
            flip_fraction: UnitRationalWire::new(1, 2).expect("one half is a unit rational"),
        }
    }
}

/// Which model one evolution stage runs on, and the stage's measured impact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct StageAllocation {
    pub stage: String,
    pub model_ref: String,
    pub impact: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RunBudget {
    pub max_cost: u64,
    pub max_stronger_stages: u32,
}

fn meets(count: usize, total: usize, fraction: UnitRationalWire) -> bool {
    // count / total >= numerator / denominator, in exact integers.
    total > 0
        && (count as u128) * u128::from(fraction.denominator())
            >= (total as u128) * u128::from(fraction.numerator())
}

/// Compare observed flips with the declared direction. Each direction is
/// tested on its own against `flip_fraction`; only an unambiguous result
/// verifies or falsifies.
pub fn decide_round_verdict(
    prediction: &PredictionDeclaration,
    outcomes: &[RoundTaskOutcome],
    defaults: &EvolutionDefaults,
) -> RoundVerdict {
    let flipped = |id: &str, to_pass: bool| {
        outcomes
            .iter()
            .any(|o| o.task_id == id && o.passed_before != to_pass && o.passed_after == to_pass)
    };
    let declared: Vec<(&str, bool)> = prediction
        .fixes
        .iter()
        .map(|id| (id.as_str(), true))
        .chain(prediction.regressions.iter().map(|id| (id.as_str(), false)))
        .collect();
    let as_declared = declared.iter().filter(|(id, up)| flipped(id, *up)).count();
    let opposite = declared.iter().filter(|(id, up)| flipped(id, !*up)).count();
    let total = declared.len();
    match (
        meets(as_declared, total, defaults.flip_fraction),
        meets(opposite, total, defaults.flip_fraction),
    ) {
        (true, false) => RoundVerdict::Verified,
        (false, true) => RoundVerdict::Falsified,
        _ => RoundVerdict::Unverified,
    }
}

#[cfg(test)]
mod tests;
