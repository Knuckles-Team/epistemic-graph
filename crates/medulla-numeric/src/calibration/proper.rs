//! Proper scoring of a binary forecaster's track record (EH-525): Brier score with
//! its Murphy decomposition, log loss, and expected calibration error.
//!
//! The Murphy decomposition groups forecasts by their DISTINCT value, which makes it
//! an identity rather than a binned approximation:
//! `brier = reliability - resolution + uncertainty`, where
//! * `reliability = Σ_k (n_k / N) (f_k - ō_k)²` (lower is better calibrated),
//! * `resolution = Σ_k (n_k / N) (ō_k - ō)²` (higher is more discriminating),
//! * `uncertainty = ō (1 - ō)` (the base rate's own variance).

use std::collections::BTreeMap;

use super::metrics::{log_loss, reliability, BinCount, ProbabilityFloor};
use super::scores::ProbabilityMatrix;
use crate::detkernel::reduce::serial_sum;
use crate::detkernel::{validate, StatResult};

/// A binary forecaster's proper scores over `n` resolved forecasts.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProperScores {
    pub n: u64,
    /// `mean (p - y)²`.
    pub brier: f64,
    /// `-mean ln max(p_y, floor)`.
    pub log_loss: f64,
    pub reliability: f64,
    pub resolution: f64,
    pub uncertainty: f64,
    /// Equal-width-bin expected calibration error of `P(event)`.
    pub expected_calibration_error: f64,
}

/// Per distinct forecast value: `(count, events)`, in value order.
fn groups(forecasts: &[f64], outcomes: &[bool]) -> BTreeMap<u64, (f64, f64)> {
    let mut out: BTreeMap<u64, (f64, f64)> = BTreeMap::new();
    for (&p, &y) in forecasts.iter().zip(outcomes) {
        let slot = out.entry(p.to_bits()).or_default();
        slot.0 += 1.0;
        slot.1 += f64::from(u8::from(y));
    }
    out
}

/// `(reliability, resolution)` of the grouped forecasts around base rate `base`.
fn murphy(groups: &BTreeMap<u64, (f64, f64)>, total: f64, base: f64) -> (f64, f64) {
    let mut reliability_terms = Vec::with_capacity(groups.len());
    let mut resolution_terms = Vec::with_capacity(groups.len());
    for (&bits, &(count, events)) in groups {
        let rate = events / count;
        let gap = f64::from_bits(bits) - rate;
        let spread = rate - base;
        reliability_terms.push(count / total * gap * gap);
        resolution_terms.push(count / total * spread * spread);
    }
    (
        serial_sum(&reliability_terms),
        serial_sum(&resolution_terms),
    )
}

/// Score forecasts `P(event)` against what happened.
pub fn binary_proper_scores(
    forecasts: &[f64],
    outcomes: &[bool],
    floor: ProbabilityFloor,
    bins: BinCount,
) -> StatResult<ProperScores> {
    validate::non_empty(forecasts, "forecasts")?;
    validate::same_len(forecasts.len(), outcomes.len(), "outcomes")?;
    validate::all_unit_interval(forecasts, "forecasts")?;
    let total = forecasts.len() as f64;
    let squares: Vec<f64> = forecasts
        .iter()
        .zip(outcomes)
        .map(|(&p, &y)| (p - f64::from(u8::from(y))) * (p - f64::from(u8::from(y))))
        .collect();
    let events: Vec<f64> = outcomes.iter().map(|&y| f64::from(u8::from(y))).collect();
    let base = serial_sum(&events) / total;
    let (reliability_term, resolution) = murphy(&groups(forecasts, outcomes), total, base);
    let rows: Vec<Vec<f64>> = forecasts.iter().map(|&p| vec![1.0 - p, p]).collect();
    let labels: Vec<usize> = outcomes.iter().map(|&y| usize::from(y)).collect();
    let matrix = ProbabilityMatrix::from_rows(&rows)?;
    Ok(ProperScores {
        n: forecasts.len() as u64,
        brier: serial_sum(&squares) / total,
        log_loss: log_loss(&matrix, &labels, floor)?,
        reliability: reliability_term,
        resolution,
        uncertainty: base * (1.0 - base),
        expected_calibration_error: reliability(forecasts, outcomes, bins)?
            .expected_calibration_error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scores(forecasts: &[f64], outcomes: &[bool]) -> ProperScores {
        let floor = ProbabilityFloor::new(1e-6).unwrap();
        binary_proper_scores(forecasts, outcomes, floor, BinCount::new(10).unwrap()).unwrap()
    }

    #[test]
    fn the_murphy_decomposition_sums_to_the_brier_score() {
        let forecasts = [0.1, 0.1, 0.7, 0.7, 0.7, 0.9, 0.3, 0.3, 0.55, 0.9];
        let outcomes = [
            false, true, true, true, false, true, false, false, true, true,
        ];
        let s = scores(&forecasts, &outcomes);
        let identity = s.reliability - s.resolution + s.uncertainty;
        assert!(
            (identity - s.brier).abs() < 1e-12,
            "{identity} vs {}",
            s.brier
        );
        assert_eq!(s.n, 10);
    }

    #[test]
    fn a_perfect_forecaster_is_reliable_and_fully_resolving() {
        let s = scores(&[0.0, 1.0, 0.0, 1.0], &[false, true, false, true]);
        assert_eq!(s.brier, 0.0);
        assert_eq!(s.reliability, 0.0);
        assert!((s.resolution - s.uncertainty).abs() < 1e-15);
        assert!(s.log_loss < 1e-5);
    }

    #[test]
    fn the_climatological_forecaster_has_no_resolution() {
        let s = scores(&[0.5; 4], &[true, false, true, false]);
        assert_eq!(s.resolution, 0.0);
        assert_eq!(s.reliability, 0.0);
        assert!((s.brier - 0.25).abs() < 1e-15);
    }

    #[test]
    fn mismatched_inputs_are_refused() {
        let floor = ProbabilityFloor::new(1e-6).unwrap();
        let bins = BinCount::new(10).unwrap();
        assert!(binary_proper_scores(&[0.5], &[true, false], floor, bins).is_err());
        assert!(binary_proper_scores(&[1.5], &[true], floor, bins).is_err());
        assert!(binary_proper_scores(&[], &[], floor, bins).is_err());
    }
}
