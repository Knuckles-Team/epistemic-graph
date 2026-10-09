// CONCEPT:EG-KG.domains.finance-compute.strategy — Strategy Evaluation Engine
//
// Versioned StrategySpec evaluation (EG-FINANCE-PRIMITIVES-R007). Evaluation is
// deterministic and emits PROPOSED ACTIONS ONLY: it never places or authorizes
// an order. This slice (R007.1) ships the dollar-cost-averaging-by-fixed-amount
// default strategy; trend-following and calendar/threshold rebalancing are later
// slices (R007.2+).

use serde::{Deserialize, Serialize};

/// Schema version of [`StrategySpec`]. Bump on any breaking field change.
pub const STRATEGY_SPEC_VERSION: u32 = 1;

/// A versioned, typed strategy specification.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StrategySpec {
    pub version: u32,
    pub kind: StrategyKind,
}

/// The default strategies named by EG-FINANCE-PRIMITIVES-R007. Only the
/// dollar-cost-averaging-by-fixed-amount variant is evaluated in this slice; the
/// others are declared here so the wire type is stable across later slices that
/// add their evaluators.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum StrategyKind {
    /// Buy a fixed cash amount (minor units, e.g. cents) on each scheduled
    /// contribution date.
    DollarCostAverageFixedAmount { amount_minor_units: i64 },
}

/// One proposed action. Evaluation never places or authorizes an order: a
/// proposal is advisory output only, consumed by a separate order-authorization
/// boundary (out of scope for this requirement).
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProposedAction {
    pub strategy_version: u32,
    pub side: OrderSide,
    pub amount_minor_units: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
}

/// Evaluate a [`StrategySpec`] for one scheduled contribution date, deterministically.
///
/// Returns `None` when the spec proposes no action (a non-positive configured
/// amount is refused rather than silently clamped).
pub fn evaluate(spec: &StrategySpec) -> Option<ProposedAction> {
    match spec.kind {
        StrategyKind::DollarCostAverageFixedAmount { amount_minor_units } => {
            if amount_minor_units <= 0 {
                return None;
            }
            Some(ProposedAction {
                strategy_version: spec.version,
                side: OrderSide::Buy,
                amount_minor_units,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dca_fixed_amount_proposes_buy_of_configured_amount() {
        let spec = StrategySpec {
            version: STRATEGY_SPEC_VERSION,
            kind: StrategyKind::DollarCostAverageFixedAmount {
                amount_minor_units: 10_000,
            },
        };
        let action = evaluate(&spec).expect("fixed-amount DCA always proposes a buy");
        assert_eq!(action.strategy_version, STRATEGY_SPEC_VERSION);
        assert_eq!(action.side, OrderSide::Buy);
        assert_eq!(action.amount_minor_units, 10_000);
    }

    #[test]
    fn dca_fixed_amount_refuses_a_non_positive_contribution() {
        let spec = StrategySpec {
            version: STRATEGY_SPEC_VERSION,
            kind: StrategyKind::DollarCostAverageFixedAmount {
                amount_minor_units: 0,
            },
        };
        assert!(evaluate(&spec).is_none());
    }

    #[test]
    fn evaluation_is_deterministic_across_repeated_calls() {
        let spec = StrategySpec {
            version: STRATEGY_SPEC_VERSION,
            kind: StrategyKind::DollarCostAverageFixedAmount {
                amount_minor_units: 5_000,
            },
        };
        assert_eq!(evaluate(&spec), evaluate(&spec));
    }
}
