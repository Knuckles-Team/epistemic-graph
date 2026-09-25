//! Walk-forward folds with a purge gap and a post-test embargo.

use std::ops::Range;

use eg_types::decision::replay::{WalkForward, MAX_REPLAY_FOLDS};
use eg_types::decision::statistical::StatisticalErrorCode;

use crate::decision::refusal::{Refusal, RefusalResult};

/// One fold: the step indices it trains on and the window it tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fold {
    pub train: Vec<usize>,
    pub test: Range<usize>,
}

fn invalid(detail: impl Into<String>) -> Refusal {
    Refusal::new(StatisticalErrorCode::ReplaySpecInvalid, detail)
}

fn embargoed(index: usize, earlier_ends: &[usize], embargo: usize) -> bool {
    earlier_ends
        .iter()
        .any(|&end| index >= end && index < end + embargo)
}

fn fold_at(start: usize, spec: &WalkForward, earlier_ends: &[usize]) -> RefusalResult<Fold> {
    let end = start - spec.purge as usize;
    let begin = end - spec.train as usize;
    let train: Vec<usize> = (begin..end)
        .filter(|&index| !embargoed(index, earlier_ends, spec.embargo as usize))
        .collect();
    if train.is_empty() {
        return Err(invalid(format!(
            "the embargo leaves the fold testing from item {start} nothing to train on"
        )));
    }
    Ok(Fold {
        train,
        test: start..start + spec.test as usize,
    })
}

/// Cut `n` time-ordered steps into walk-forward folds. Test windows never
/// overlap (`step >= test`), so every step is replayed at most once.
pub fn walk_forward(n: usize, spec: &WalkForward) -> RefusalResult<Vec<Fold>> {
    if spec.train == 0 || spec.test == 0 || spec.step < spec.test {
        return Err(invalid("train and test must be positive and step >= test"));
    }
    let mut folds: Vec<Fold> = Vec::new();
    let mut start = (spec.train + spec.purge) as usize;
    while start + spec.test as usize <= n {
        let earlier_ends: Vec<usize> = folds.iter().map(|fold| fold.test.end).collect();
        folds.push(fold_at(start, spec, &earlier_ends)?);
        start += spec.step as usize;
    }
    if folds.is_empty() {
        return Err(invalid(format!("{n} items leave no walk-forward fold")));
    }
    if folds.len() > MAX_REPLAY_FOLDS {
        return Err(invalid(format!(
            "{} folds exceed the bound of {MAX_REPLAY_FOLDS}",
            folds.len()
        )));
    }
    Ok(folds)
}
