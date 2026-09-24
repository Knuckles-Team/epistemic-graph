//! Forecaster track records (EH-525): a fitted decision head is a probabilistic
//! forecaster, and each independently evaluated execution of a decision it made is one
//! resolved forecast — the head's calibrated probability for the executed option,
//! against whether that option succeeded. The records are scored with proper scoring
//! rules ([`crate::calibration::proper`]); the reputation view reports them per head.

use std::collections::BTreeMap;

use eg_types::decision::statistical::head::DecisionHeadBody;

use super::features::FeatureMatrix;
use super::head_eval::{read_head, HeadReading};
use super::refusal::RefusalResult;
use crate::calibration::{binary_proper_scores, BinCount, ProbabilityFloor, ProperScores};
use crate::detkernel::StatResult;
use crate::risk::SampleGate;

/// The head's probability for `executed` over `matrix`, re-read from the record's own
/// inputs; `None` when the head scores without probabilities (a weighted-features head),
/// the row is out of the head's distribution, or `executed` is not a candidate.
pub fn executed_forecast(
    head: &DecisionHeadBody,
    matrix: &FeatureMatrix,
    executed: &str,
) -> RefusalResult<Option<f64>> {
    let HeadReading::InDistribution(evaluated) = read_head(head, matrix)? else {
        return Ok(None);
    };
    let Some(probabilities) = evaluated.probabilities else {
        return Ok(None);
    };
    Ok(matrix
        .candidate_ids
        .iter()
        .position(|c| c == executed)
        .map(|index| probabilities[index]))
}

/// One resolved forecast of `forecaster`.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedForecast {
    pub forecaster: String,
    pub probability: f64,
    pub success: bool,
}

/// A forecaster's track record: its proper scores, or `None` below the gate.
#[derive(Debug, Clone, PartialEq)]
pub struct TrackRecord {
    pub forecaster: String,
    pub n: u64,
    /// Forecasts whose option succeeded.
    pub successes: u64,
    pub scores: Option<ProperScores>,
}

/// Score every forecaster's resolved forecasts, in forecaster order; below `gate` a
/// forecaster gets no scores (a small record is not a track record).
pub fn track_records(
    forecasts: &[ResolvedForecast],
    gate: SampleGate,
) -> StatResult<Vec<TrackRecord>> {
    let mut by: BTreeMap<&str, (Vec<f64>, Vec<bool>)> = BTreeMap::new();
    for forecast in forecasts {
        let slot = by.entry(forecast.forecaster.as_str()).or_default();
        slot.0.push(forecast.probability);
        slot.1.push(forecast.success);
    }
    let floor = ProbabilityFloor::new(1e-6)?;
    let bins = BinCount::new(10)?;
    by.into_iter()
        .map(|(forecaster, (probabilities, outcomes))| {
            let n = probabilities.len() as u64;
            let scores = gate
                .admits(n)
                .then(|| binary_proper_scores(&probabilities, &outcomes, floor, bins))
                .transpose()?;
            Ok(TrackRecord {
                forecaster: forecaster.to_string(),
                n,
                successes: outcomes.iter().filter(|&&y| y).count() as u64,
                scores,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn forecast(forecaster: &str, probability: f64, success: bool) -> ResolvedForecast {
        ResolvedForecast {
            forecaster: forecaster.into(),
            probability,
            success,
        }
    }

    #[test]
    fn records_are_scored_per_forecaster_and_gated() {
        let forecasts = vec![
            forecast("h1", 0.9, true),
            forecast("h1", 0.8, true),
            forecast("h1", 0.3, false),
            forecast("h2", 0.6, true),
        ];
        let records = track_records(&forecasts, SampleGate::new(2).unwrap()).unwrap();
        assert_eq!(records.len(), 2);
        let h1 = records[0].scores.unwrap();
        let brier = (0.01 + 0.04 + 0.09) / 3.0;
        assert!((h1.brier - brier).abs() < 1e-12);
        assert_eq!(records[1].n, 1);
        assert!(
            records[1].scores.is_none(),
            "one forecast is below the gate"
        );
    }
}
