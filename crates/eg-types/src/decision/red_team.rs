//! Red-team attack, transform and scorer records and the probe verdict
//! (MEDULLA-DECISION-ENGINE-R138.1 and R138.2).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RedTeamAttack {
    pub id: String,
    pub category: String,
    pub goal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RedTeamTransform {
    pub id: String,
    pub kind: String,
}

/// A scorer passes when its score reaches `threshold` (both per mille).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RedTeamScorer {
    pub id: String,
    pub category: String,
    pub threshold: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ProbeVerdict {
    Resisted,
    Compromised,
    Inconclusive,
}

/// One selected scorer's result. `score` is absent when the scorer gave none.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ScorerResult {
    pub scorer: RedTeamScorer,
    pub score: Option<u16>,
}

/// The named, replaceable defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RedTeamPolicy {
    /// Select only scorers whose category equals the attack category.
    pub category_match: bool,
    /// One passing scorer is enough to call the probe compromised.
    pub any_scorer_compromises: bool,
}

impl Default for RedTeamPolicy {
    fn default() -> Self {
        Self {
            category_match: true,
            any_scorer_compromises: true,
        }
    }
}

pub fn select_scorers<'a>(
    attack: &RedTeamAttack,
    scorers: &'a [RedTeamScorer],
    policy: &RedTeamPolicy,
) -> Vec<&'a RedTeamScorer> {
    scorers
        .iter()
        .filter(|scorer| !policy.category_match || scorer.category == attack.category)
        .collect()
}

/// `compromised` when a selected scorer passes (any one under the default,
/// all of them otherwise), `resisted` when every scorer fails, and
/// `inconclusive` when there is no result or a score is missing.
pub fn decide_probe_verdict(results: &[ScorerResult], policy: &RedTeamPolicy) -> ProbeVerdict {
    if results.is_empty() || results.iter().any(|r| r.score.is_none()) {
        return ProbeVerdict::Inconclusive;
    }
    let passes = |r: &ScorerResult| r.score.is_some_and(|s| s >= r.scorer.threshold);
    let compromised = if policy.any_scorer_compromises {
        results.iter().any(passes)
    } else {
        results.iter().all(passes)
    };
    if compromised {
        ProbeVerdict::Compromised
    } else if results.iter().all(|r| !passes(r)) {
        ProbeVerdict::Resisted
    } else {
        ProbeVerdict::Inconclusive
    }
}

#[cfg(test)]
mod tests;
