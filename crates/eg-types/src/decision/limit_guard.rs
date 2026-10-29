//! Limit guard over a proposed action (MEDULLA-DECISION-ENGINE-R131.1 and R131.2).
//!
//! Each rule names one closed metric, a bound and the scope it covers. A
//! request may supply its own bounds, but only a tighter bound takes effect.

use serde::{Deserialize, Serialize};

/// The closed metric set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum LimitMetric {
    PositionSize,
    Drawdown,
    SectorConcentration,
}

impl LimitMetric {
    /// The named default check for this metric: an observed value above the
    /// bound is a breach. Every current metric is an upper bound.
    pub fn breaches(self, observed: i64, bound: i64) -> bool {
        match self {
            Self::PositionSize | Self::Drawdown | Self::SectorConcentration => observed > bound,
        }
    }
}

/// One configured limit. Values are integer units of the metric (for example
/// minor currency units or basis points).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LimitRule {
    pub id: String,
    pub metric: LimitMetric,
    pub bound: i64,
    pub scope: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LimitBreach {
    pub rule_id: String,
    pub observed: i64,
    pub bound: i64,
    pub signal_iri: String,
}

/// One observed metric value of the proposed action, in one scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LimitObservation {
    pub metric: LimitMetric,
    pub scope: String,
    pub value: i64,
}

/// A request-supplied bound for one rule. It can only tighten that rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RequestedBound {
    pub rule_id: String,
    pub bound: i64,
}

/// The proposed action as the guard sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GuardedAction {
    pub signal_iri: String,
    pub observations: Vec<LimitObservation>,
    #[serde(default)]
    pub requested_bounds: Vec<RequestedBound>,
}

fn effective_bound(rule: &LimitRule, action: &GuardedAction) -> i64 {
    action
        .requested_bounds
        .iter()
        .filter(|requested| requested.rule_id == rule.id)
        .map(|requested| requested.bound)
        .fold(rule.bound, i64::min)
}

/// Every breach of `rules` by `action`, in rule order.
pub fn check_limits(action: &GuardedAction, rules: &[LimitRule]) -> Vec<LimitBreach> {
    rules
        .iter()
        .filter_map(|rule| {
            let bound = effective_bound(rule, action);
            action
                .observations
                .iter()
                .filter(|o| o.metric == rule.metric && o.scope == rule.scope)
                .map(|o| o.value)
                .max()
                .filter(|observed| rule.metric.breaches(*observed, bound))
                .map(|observed| LimitBreach {
                    rule_id: rule.id.clone(),
                    observed,
                    bound,
                    signal_iri: action.signal_iri.clone(),
                })
        })
        .collect()
}

#[cfg(test)]
mod tests;
