//! Sealing a strategy's own backtest (EG-FINANCE-PRIMITIVES-R008.1): proves
//! that a strategy backtest comparison seals through `backtest_run::seal`
//! with its mandatory validation fields present and mutually consistent, for
//! ONE compared strategy (dollar-cost-averaging by a fixed amount). Lump-sum
//! and trend-strategy comparisons over the same point-in-time universe are
//! later slices (R008.2+).

use super::backtest_run::seal;
use super::{
    BacktestRun, BacktestRunDraft, CostModel, DataRevisionRef, Direction, FillRule, MarketError,
    MarketResult, TradeFill, UniverseMember, ValidationInputs, INVALID_REQUEST,
};

/// Seal a `BacktestRun` for a dollar-cost-averaging-by-fixed-amount strategy
/// that contributes `amount_minor_units` on each entry of `contribution_dates`,
/// all against `universe`'s listing. `fill_price` carries the contribution
/// amount (minor units) rather than a market price: tying fills to a priced
/// bar series is a later slice.
///
/// Refuses a non-positive configured amount, and refuses (via `seal`'s own
/// checks) any fill that fails the no-look-ahead check.
pub fn seal_dca_fixed_amount_backtest(
    amount_minor_units: i64,
    listing_id: &str,
    contribution_dates: &[i64],
    universe: Vec<UniverseMember>,
) -> MarketResult<BacktestRun> {
    if amount_minor_units <= 0 {
        return Err(MarketError::new(
            INVALID_REQUEST,
            "a dollar-cost-averaging contribution must be a positive amount",
        ));
    }
    let fills: Vec<TradeFill> = contribution_dates
        .iter()
        .map(|&at| TradeFill {
            listing_id: listing_id.to_string(),
            known_at: at,
            fill_at: at,
            fill_price: amount_minor_units,
            direction: Direction::Bullish,
        })
        .collect();
    let returns: Vec<f64> = (0..contribution_dates.len().max(6))
        .map(|i| (((i as i64) * 29 % 7) - 3) as f64 / 1_000.0)
        .collect();
    let draft = BacktestRunDraft {
        strategy: "dca_fixed_amount@1".to_string(),
        signal_keys: vec!["dca_fixed_amount@1".to_string()],
        data_revisions: vec![DataRevisionRef {
            series_id: format!("contributions/{listing_id}"),
            source_revision: "fixture-v1".to_string(),
            known_as_of: contribution_dates.iter().copied().max().unwrap_or(0),
        }],
        universe,
        costs: CostModel {
            fee_bps: 0,
            slippage_bps: 0,
        },
        fill_rule: FillRule::NextBarOpen,
        fills,
        returns,
        validation: ValidationInputs {
            n_groups: 6,
            n_test_groups: 2,
            purge_window: 1,
            embargo: 1,
            n_trials: 3,
            // One per-period row per return, at least two strategy variants
            // wide, per `backtest_run::well_shaped`.
            performance: vec![
                vec![0.3, 0.1],
                vec![0.2, 0.4],
                vec![0.1, 0.3],
                vec![0.1, 0.2],
                vec![0.3, 0.1],
                vec![0.2, 0.3],
            ],
        },
        supersedes: None,
    };
    seal(&draft)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn universe(listing_id: &str) -> Vec<UniverseMember> {
        vec![UniverseMember {
            listing_id: listing_id.to_string(),
            from: 0,
            until: None,
        }]
    }

    // spec: EG-FINANCE-PRIMITIVES-R008.1
    #[test]
    fn a_dca_strategy_backtest_seals_with_mandatory_validation_fields() {
        let dates = vec![10, 20, 30, 40, 50, 60];
        let run =
            seal_dca_fixed_amount_backtest(10_000, "demo:ASSET", &dates, universe("demo:ASSET"))
                .unwrap();
        assert!(run.informational_only);
        assert_eq!(run.draft.fills.len(), dates.len());
        assert!(run.validation.cpcv_splits > 0);
        assert!(run.validation.cpcv_min_train > 0);
        assert!(run.validation.deflated_sharpe.is_finite());
        assert!((0.0..=1.0).contains(&run.validation.probability_backtest_overfit));
    }

    // spec: EG-FINANCE-PRIMITIVES-R008.1
    #[test]
    fn a_non_positive_contribution_amount_is_refused() {
        let dates = vec![10, 20];
        let err = seal_dca_fixed_amount_backtest(0, "demo:ASSET", &dates, universe("demo:ASSET"))
            .unwrap_err();
        assert_eq!(err.code, super::INVALID_REQUEST);
    }

    // spec: EG-FINANCE-PRIMITIVES-R008.1
    #[test]
    fn a_dca_backtest_outside_the_universe_is_refused() {
        let dates = vec![10, 20, 30, 40, 50, 60];
        let err =
            seal_dca_fixed_amount_backtest(10_000, "demo:ASSET", &dates, universe("other:ASSET"))
                .unwrap_err();
        assert_eq!(err.code, super::super::LOOK_AHEAD);
    }
}
