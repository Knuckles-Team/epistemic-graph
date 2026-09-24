//! The backtest-run provenance record (EH-418).
//!
//! A run is sealed only when its provenance is complete (signal keys, data
//! revisions, a point-in-time universe, costs and a fill rule) and every fill
//! passes the no-look-ahead check: the signal was knowable at or before the
//! fill, and the listing was in the universe at the fill. The mandatory
//! validation outputs come from the engine's existing kernels — purged
//! combinatorial CV, the deflated Sharpe ratio and the probability of backtest
//! overfitting — never from the caller. The record is content-addressed and
//! immutable; a revised run is a new record that `supersedes` the old digest.

use super::digest;
use super::{
    BacktestRun, BacktestRunDraft, BacktestValidation, MarketError, MarketResult, TradeFill,
    UniverseMember, INVALID_REQUEST, LOOK_AHEAD,
};
use crate::finance::quant::{
    deflated_sharpe_ratio, probability_of_backtest_overfit, purged_cpcv_splits,
};

const RUN_DOMAIN: &str = "eg/finance/backtest-run/v1";

fn refuse(detail: impl Into<String>) -> MarketError {
    MarketError::new(INVALID_REQUEST, detail)
}

fn all_finite(values: &[f64]) -> bool {
    values.iter().all(|value| value.is_finite())
}

fn check_provenance(draft: &BacktestRunDraft) -> MarketResult<()> {
    let complete = !draft.strategy.is_empty()
        && !draft.signal_keys.is_empty()
        && !draft.data_revisions.is_empty()
        && !draft.universe.is_empty();
    if !complete {
        return Err(refuse(
            "a backtest run needs a strategy, signal keys, data revisions and a universe",
        ));
    }
    Ok(())
}

fn well_shaped(insample: &[Vec<f64>], oos: &[Vec<f64>]) -> bool {
    let width = insample.first().map_or(0, Vec::len);
    let rows_ok = insample
        .iter()
        .chain(oos)
        .all(|row| row.len() == width && all_finite(row));
    !insample.is_empty() && insample.len() == oos.len() && width >= 2 && rows_ok
}

fn check_matrices(draft: &BacktestRunDraft) -> MarketResult<()> {
    let inputs = &draft.validation;
    if !well_shaped(&inputs.insample, &inputs.oos) {
        return Err(refuse(
            "in-sample and out-of-sample matrices must be non-empty, equal-shaped, finite, with >= 2 variants",
        ));
    }
    Ok(())
}

fn check_groups(draft: &BacktestRunDraft) -> MarketResult<()> {
    let inputs = &draft.validation;
    let groups_ok = inputs.n_groups >= 2
        && (1..inputs.n_groups).contains(&inputs.n_test_groups)
        && inputs.n_trials >= 1;
    let enough_returns = draft.returns.len() >= inputs.n_groups.max(4) as usize;
    if !(groups_ok && enough_returns && all_finite(&draft.returns)) {
        return Err(refuse(
            "n_groups >= 2, 1 <= n_test_groups < n_groups, n_trials >= 1 and >= max(n_groups, 4) finite returns are required",
        ));
    }
    Ok(())
}

fn member_at(universe: &[UniverseMember], fill: &TradeFill) -> bool {
    universe.iter().any(|member| {
        member.listing_id == fill.listing_id
            && member.from <= fill.fill_at
            && member.until.is_none_or(|until| fill.fill_at < until)
    })
}

/// No fill before its signal was knowable, none outside the universe of its time.
pub fn check_no_look_ahead(draft: &BacktestRunDraft) -> MarketResult<()> {
    for fill in &draft.fills {
        if fill.known_at > fill.fill_at {
            return Err(MarketError::new(
                LOOK_AHEAD,
                format!(
                    "fill of {} at {} precedes its signal (known {})",
                    fill.listing_id, fill.fill_at, fill.known_at
                ),
            ));
        }
        if !member_at(&draft.universe, fill) {
            return Err(MarketError::new(
                LOOK_AHEAD,
                format!(
                    "{} was not in the point-in-time universe at {}",
                    fill.listing_id, fill.fill_at
                ),
            ));
        }
    }
    Ok(())
}

/// Per-period Sharpe ratio: mean over sample standard deviation.
fn sharpe(returns: &[f64]) -> MarketResult<f64> {
    let n = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let variance = returns.iter().map(|r| (r - mean) * (r - mean)).sum::<f64>() / (n - 1.0);
    if variance <= 0.0 {
        return Err(refuse(
            "returns have no variance; a Sharpe ratio is undefined",
        ));
    }
    Ok(mean / variance.sqrt())
}

fn validation_outputs(draft: &BacktestRunDraft) -> MarketResult<BacktestValidation> {
    let inputs = &draft.validation;
    let splits = purged_cpcv_splits(
        draft.returns.len(),
        inputs.n_groups as usize,
        inputs.n_test_groups as usize,
        inputs.purge_window as usize,
        inputs.embargo as usize,
    );
    let min_train = splits
        .iter()
        .map(|split| split.train.len())
        .min()
        .unwrap_or(0);
    if min_train == 0 {
        return Err(refuse("purged CPCV leaves a split without training data"));
    }
    let observed_sharpe = sharpe(&draft.returns)?;
    Ok(BacktestValidation {
        cpcv_splits: splits.len() as u32,
        cpcv_min_train: min_train as u32,
        observed_sharpe,
        deflated_sharpe: deflated_sharpe_ratio(
            observed_sharpe,
            inputs.n_trials as usize,
            &draft.returns,
        ),
        probability_backtest_overfit: probability_of_backtest_overfit(
            &inputs.insample,
            &inputs.oos,
        ),
    })
}

/// Validate a draft and seal it as an immutable, content-addressed record.
pub fn seal(draft: &BacktestRunDraft) -> MarketResult<BacktestRun> {
    check_provenance(draft)?;
    check_matrices(draft)?;
    check_groups(draft)?;
    check_no_look_ahead(draft)?;
    let validation = validation_outputs(draft)?;
    Ok(BacktestRun {
        digest: digest::of_json(RUN_DOMAIN, &(draft, &validation)),
        draft: draft.clone(),
        validation,
        informational_only: true,
    })
}

/// Re-derive a record from its own draft: it verifies only if every output and
/// the digest reproduce exactly.
pub fn verify(record: &BacktestRun) -> MarketResult<bool> {
    Ok(seal(&record.draft)? == *record)
}
