//! Decide-calibrated flip confidence with conformal abstention (EH-417).
//!
//! For one candidate flip at one horizon, the score is the smoothed
//! follow-through rate of the flip's feature cell (timeframe agreement, 200-week
//! position, regime) among historical flips of the same direction. The Decide
//! layer's own kernels do the rest: the minimum-sample gate for the conformal
//! level (`SampleGate::for_conformal`), a Clopper-Pearson drift check between
//! the earlier and recent halves of the history, and a class-conditional binary
//! conformal set calibrated on leave-one-out scores. Only a singleton set is a
//! claim; warm-up, thin history, an unsupported regime, drift and an ambiguous
//! set each abstain with a typed reason. It never invents a confidence.

use std::collections::BTreeMap;

use eg_numeric::conformal::{binary_conformal, BinarySet};
use eg_numeric::detkernel::{Level, StatError};
use eg_numeric::risk::{clopper_pearson, BinomialCounts, IntervalSide, SampleGate};

use super::{
    DataStatus, FlipAbstainReason, FlipConfidence, FlipConfidenceRequest, FlipFeatures,
    FlipOutcomeSample, MarketError, MarketResult, INVALID_REQUEST,
};

fn refused(error: StatError) -> MarketError {
    MarketError::new(INVALID_REQUEST, error.to_string())
}

type Cell = (bool, bool, u32);

fn cell(features: &FlipFeatures) -> Cell {
    (
        features.timeframe_agreement,
        features.above_200w_sma,
        features.regime,
    )
}

/// Laplace-smoothed rate `(successes + 1) / (trials + 2)`.
fn smoothed(successes: u64, trials: u64) -> f64 {
    (successes as f64 + 1.0) / (trials as f64 + 2.0)
}

/// Per-cell `(successes, trials)`.
fn tally(samples: &[FlipOutcomeSample]) -> BTreeMap<Cell, (u64, u64)> {
    let mut cells = BTreeMap::new();
    for sample in samples {
        let entry = cells.entry(cell(&sample.features)).or_insert((0, 0));
        entry.0 += u64::from(sample.followed_through);
        entry.1 += 1;
    }
    cells
}

/// Each sample's score with itself left out of its cell.
fn leave_one_out(samples: &[FlipOutcomeSample], cells: &BTreeMap<Cell, (u64, u64)>) -> Vec<f64> {
    samples
        .iter()
        .map(|sample| {
            let (successes, trials) = cells[&cell(&sample.features)];
            smoothed(successes - u64::from(sample.followed_through), trials - 1)
        })
        .collect()
}

fn rate(samples: &[FlipOutcomeSample]) -> (u64, u64) {
    let successes = samples.iter().filter(|s| s.followed_through).count() as u64;
    (successes, samples.len() as u64)
}

/// The recent half's rate outside the earlier half's interval is drift.
fn drift(samples: &[FlipOutcomeSample], alpha: Level) -> MarketResult<Option<FlipAbstainReason>> {
    let (earlier, recent) = samples.split_at(samples.len() / 2);
    let (successes, trials) = rate(earlier);
    let counts = BinomialCounts::new(successes, trials).map_err(refused)?;
    let interval = clopper_pearson(counts, alpha, IntervalSide::TwoSided).map_err(refused)?;
    let (recent_successes, recent_trials) = rate(recent);
    let recent_rate = recent_successes as f64 / recent_trials as f64;
    let inside = interval.lower <= recent_rate && recent_rate <= interval.upper;
    Ok((!inside).then_some(FlipAbstainReason::Drift {
        earlier_lower: interval.lower,
        earlier_upper: interval.upper,
        recent_rate,
    }))
}

fn validate(request: &FlipConfidenceRequest) -> MarketResult<Level> {
    let valid = request.horizon_bars >= 1
        && (1..=500).contains(&request.alpha_permille)
        && request.n_min >= 1;
    if !valid {
        return Err(MarketError::new(
            INVALID_REQUEST,
            "horizon_bars >= 1, alpha_permille in 1..=500 and n_min >= 1 are required",
        ));
    }
    Level::new(u64::from(request.alpha_permille), 1_000).map_err(refused)
}

/// The abstention that applies before any score is computed, if any.
fn gate_reason(
    request: &FlipConfidenceRequest,
    samples: &[FlipOutcomeSample],
    gate: SampleGate,
    alpha: Level,
) -> MarketResult<Option<FlipAbstainReason>> {
    if request.data_status != DataStatus::Valid {
        return Ok(Some(FlipAbstainReason::DataNotValid {
            data_status: request.data_status,
        }));
    }
    let n = samples.len() as u64;
    if !gate.admits(n) {
        return Ok(Some(FlipAbstainReason::InsufficientHistory {
            n,
            n_min: gate.n_min(),
        }));
    }
    let regime = request.features.regime;
    let in_regime = samples
        .iter()
        .filter(|s| s.features.regime == regime)
        .count() as u64;
    if in_regime < u64::from(request.n_min) {
        return Ok(Some(FlipAbstainReason::RegimeUnsupported {
            regime,
            n: in_regime,
            n_min: u64::from(request.n_min),
        }));
    }
    drift(samples, alpha)
}

/// Calibrated follow-through for one flip, or a typed abstention.
pub fn flip_confidence(request: &FlipConfidenceRequest) -> MarketResult<FlipConfidence> {
    let alpha = validate(request)?;
    let gate = SampleGate::for_conformal(alpha, u64::from(request.n_min)).map_err(refused)?;
    let mut samples: Vec<FlipOutcomeSample> = request
        .history
        .iter()
        .filter(|sample| sample.direction == request.direction)
        .copied()
        .collect();
    samples.sort_by_key(|sample| {
        (
            sample.effective_at,
            cell(&sample.features),
            sample.followed_through,
        )
    });
    let horizon_bars = request.horizon_bars;
    if let Some(reason) = gate_reason(request, &samples, gate, alpha)? {
        return Ok(FlipConfidence::Abstained {
            horizon_bars,
            reason,
        });
    }
    let cells = tally(&samples);
    let outcomes: Vec<bool> = samples.iter().map(|s| s.followed_through).collect();
    let scores = leave_one_out(&samples, &cells);
    let (successes, trials) = cells
        .get(&cell(&request.features))
        .copied()
        .unwrap_or((0, 0));
    let probability = smoothed(successes, trials);
    let conformal = binary_conformal(&scores, &outcomes, alpha, gate).map_err(refused)?;
    let follows_through = match conformal.predict(probability).map_err(refused)? {
        BinarySet::Positive => true,
        BinarySet::Negative => false,
        BinarySet::Both | BinarySet::Empty => {
            return Ok(FlipConfidence::Abstained {
                horizon_bars,
                reason: FlipAbstainReason::AmbiguousSet,
            })
        }
    };
    Ok(FlipConfidence::Calibrated {
        horizon_bars,
        probability,
        follows_through,
        alpha_permille: request.alpha_permille,
        n_calibration: samples.len() as u64,
    })
}
