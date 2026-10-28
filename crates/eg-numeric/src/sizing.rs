//! Position sizing and distribution-shift detection (EG-FINANCE-PRIMITIVES-R900).
//!
//! `kelly_fraction` is the fractional-Kelly stake `(p * b - q) / b`, scaled by a
//! fraction and clamped to `[0, max_risk]`; `distribution_shift` reports a shift
//! when the two-sample Kolmogorov-Smirnov statistic exceeds a threshold. Both are
//! pure, use only correctly rounded IEEE operations, and refuse bad input with a
//! stable code rather than returning a fabricated number.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Default Kelly fraction: half Kelly.
pub const DEFAULT_KELLY_FRACTION: f64 = 0.5;
/// Default per-position risk cap: two percent of capital.
pub const DEFAULT_MAX_RISK: f64 = 0.02;

/// A refused sizing or shift-detection request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizingError {
    /// A non-finite or out-of-range input; `field` names it.
    InputInvalid { field: &'static str },
    /// A shift-detection sample was empty.
    SampleEmpty,
}

impl SizingError {
    /// The stable wire code of this refusal.
    pub fn code(&self) -> &'static str {
        match self {
            SizingError::InputInvalid { .. } => "SIZING_INPUT_INVALID",
            SizingError::SampleEmpty => "SHIFT_SAMPLE_EMPTY",
        }
    }
}

impl fmt::Display for SizingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SizingError::InputInvalid { field } => {
                write!(f, "{}: {field} is not a finite in-range value", self.code())
            }
            SizingError::SampleEmpty => write!(f, "{}: sample is empty", self.code()),
        }
    }
}

impl std::error::Error for SizingError {}

/// Inputs of one fractional-Kelly sizing.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct KellyInput {
    /// Probability of a winning bet, in `[0, 1]`.
    pub accuracy: f64,
    /// Average win divided by average loss.
    pub win_loss_ratio: f64,
    /// Fraction of full Kelly to stake.
    pub fraction: f64,
    /// Hard cap on the returned stake.
    pub max_risk: f64,
}

impl KellyInput {
    /// Validate every field: all finite, `accuracy` in `[0, 1]`, and a
    /// non-negative `fraction` and `max_risk`.
    pub fn new(
        accuracy: f64,
        win_loss_ratio: f64,
        fraction: f64,
        max_risk: f64,
    ) -> Result<Self, SizingError> {
        if !accuracy.is_finite() || !(0.0..=1.0).contains(&accuracy) {
            return Err(SizingError::InputInvalid { field: "accuracy" });
        }
        if !win_loss_ratio.is_finite() {
            return Err(SizingError::InputInvalid {
                field: "win_loss_ratio",
            });
        }
        if !fraction.is_finite() || fraction < 0.0 {
            return Err(SizingError::InputInvalid { field: "fraction" });
        }
        if !max_risk.is_finite() || max_risk < 0.0 {
            return Err(SizingError::InputInvalid { field: "max_risk" });
        }
        Ok(Self {
            accuracy,
            win_loss_ratio,
            fraction,
            max_risk,
        })
    }

    /// Half Kelly capped at two percent.
    pub fn with_defaults(accuracy: f64, win_loss_ratio: f64) -> Result<Self, SizingError> {
        Self::new(
            accuracy,
            win_loss_ratio,
            DEFAULT_KELLY_FRACTION,
            DEFAULT_MAX_RISK,
        )
    }
}

/// The fractional-Kelly stake `(p * b - q) / b * fraction`, clamped to
/// `[0, max_risk]`. A win/loss ratio of zero or less returns zero.
pub fn kelly_fraction(input: &KellyInput) -> f64 {
    let b = input.win_loss_ratio;
    if b <= 0.0 {
        return 0.0;
    }
    let q = 1.0 - input.accuracy;
    let full = (input.accuracy * b - q) / b;
    (full * input.fraction).clamp(0.0, input.max_risk)
}

/// The outcome of a two-sample shift test.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ShiftResult {
    /// The Kolmogorov-Smirnov statistic `D`.
    pub statistic: f64,
    pub p_value: f64,
    /// Whether `statistic` exceeded the threshold.
    pub shifted: bool,
}

/// Report a distribution shift between `reference` and `current` when their
/// two-sample KS statistic exceeds `threshold`. O(n log n).
#[cfg(feature = "analytics")]
pub fn distribution_shift(
    reference: &[f64],
    current: &[f64],
    threshold: f64,
) -> Result<ShiftResult, SizingError> {
    if reference.is_empty() || current.is_empty() {
        return Err(SizingError::SampleEmpty);
    }
    if !threshold.is_finite() {
        return Err(SizingError::InputInvalid { field: "threshold" });
    }
    if reference.iter().chain(current).any(|v| !v.is_finite()) {
        return Err(SizingError::InputInvalid { field: "sample" });
    }
    let (statistic, p_value) = crate::stats::ks_2samp(
        ndarray::ArrayView1::from(reference),
        ndarray::ArrayView1::from(current),
    )
    .map_err(|_| SizingError::SampleEmpty)?;
    Ok(ShiftResult {
        statistic,
        p_value,
        shifted: statistic > threshold,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full_kelly(accuracy: f64, ratio: f64) -> KellyInput {
        KellyInput::new(accuracy, ratio, 1.0, 1.0).unwrap()
    }

    // spec: EG-FINANCE-PRIMITIVES-R900.1
    #[test]
    fn kelly_input_round_trips_and_defaults_to_half_kelly_with_a_two_percent_cap() {
        let input = KellyInput::with_defaults(0.6, 2.0).unwrap();
        assert_eq!(input.fraction, 0.5);
        assert_eq!(input.max_risk, 0.02);
        let json = serde_json::to_string(&input).unwrap();
        assert_eq!(serde_json::from_str::<KellyInput>(&json).unwrap(), input);
        let shift = ShiftResult {
            statistic: 0.25,
            p_value: 0.5,
            shifted: false,
        };
        let json = serde_json::to_string(&shift).unwrap();
        assert_eq!(serde_json::from_str::<ShiftResult>(&json).unwrap(), shift);
    }

    // spec: EG-FINANCE-PRIMITIVES-R900.1
    #[test]
    fn accuracy_outside_the_unit_interval_is_rejected() {
        for bad in [-0.01, 1.01, f64::NAN, f64::INFINITY] {
            let err = KellyInput::with_defaults(bad, 1.0).unwrap_err();
            assert_eq!(err, SizingError::InputInvalid { field: "accuracy" });
        }
        assert!(KellyInput::with_defaults(0.0, 1.0).is_ok());
        assert!(KellyInput::with_defaults(1.0, 1.0).is_ok());
    }

    // spec: EG-FINANCE-PRIMITIVES-R900.2
    #[test]
    fn kelly_fraction_matches_published_values() {
        // Thorp / Kelly: even-money bet at 60% wins stakes 20% of capital.
        assert!((kelly_fraction(&full_kelly(0.6, 1.0)) - 0.2).abs() < 1e-12);
        // 60% at 2:1 payoff: (0.6*2 - 0.4)/2 = 0.4.
        assert!((kelly_fraction(&full_kelly(0.6, 2.0)) - 0.4).abs() < 1e-12);
        // 55% even money: 10%.
        assert!((kelly_fraction(&full_kelly(0.55, 1.0)) - 0.1).abs() < 1e-12);
    }

    // spec: EG-FINANCE-PRIMITIVES-R900.2
    #[test]
    fn kelly_fraction_scales_clamps_and_zeroes_out() {
        // Half of 0.2 is 0.1, but the default two percent cap wins.
        assert_eq!(
            kelly_fraction(&KellyInput::with_defaults(0.6, 1.0).unwrap()),
            0.02
        );
        // Half Kelly, generous cap: 0.1.
        let half = KellyInput::new(0.6, 1.0, 0.5, 1.0).unwrap();
        assert!((kelly_fraction(&half) - 0.1).abs() < 1e-12);
        // Negative edge never stakes.
        assert_eq!(kelly_fraction(&full_kelly(0.4, 1.0)), 0.0);
        // A ratio of zero or less returns zero.
        assert_eq!(kelly_fraction(&full_kelly(0.9, 0.0)), 0.0);
        assert_eq!(kelly_fraction(&full_kelly(0.9, -3.0)), 0.0);
        // Pure: repeat calls are identical.
        let input = full_kelly(0.6, 2.0);
        assert_eq!(
            kelly_fraction(&input).to_bits(),
            kelly_fraction(&input).to_bits()
        );
    }

    #[cfg(feature = "analytics")]
    // spec: EG-FINANCE-PRIMITIVES-R900.2
    #[test]
    fn distribution_shift_reports_a_shift_above_the_threshold() {
        let same = distribution_shift(&[1.0, 2.0, 3.0], &[1.0, 2.0, 3.0], 0.5).unwrap();
        assert_eq!(same.statistic, 0.0);
        assert!(!same.shifted);
        // Disjoint supports: D = 1 exactly (matches scipy.stats.ks_2samp).
        let apart = distribution_shift(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0], 0.5).unwrap();
        assert_eq!(apart.statistic, 1.0);
        assert!(apart.shifted);
        // One tied value moves D to 2/3 for [1,2,3] vs [3,4,5].
        let partial = distribution_shift(&[1.0, 2.0, 3.0], &[3.0, 4.0, 5.0], 0.9).unwrap();
        assert!((partial.statistic - 2.0 / 3.0).abs() < 1e-12);
        assert!(!partial.shifted);
    }

    // spec: EG-FINANCE-PRIMITIVES-R900.4
    #[test]
    fn non_finite_sizing_input_is_refused_with_its_code() {
        let cases = [
            KellyInput::new(f64::NAN, 1.0, 0.5, 0.02),
            KellyInput::new(0.5, f64::NAN, 0.5, 0.02),
            KellyInput::new(0.5, f64::INFINITY, 0.5, 0.02),
            KellyInput::new(0.5, 1.0, f64::NAN, 0.02),
            KellyInput::new(0.5, 1.0, 0.5, f64::INFINITY),
        ];
        for case in cases {
            assert_eq!(case.unwrap_err().code(), "SIZING_INPUT_INVALID");
        }
    }

    #[cfg(feature = "analytics")]
    // spec: EG-FINANCE-PRIMITIVES-R900.4
    #[test]
    fn empty_or_non_finite_shift_samples_are_refused_with_their_codes() {
        let empty = distribution_shift(&[], &[1.0], 0.5).unwrap_err();
        assert_eq!(empty.code(), "SHIFT_SAMPLE_EMPTY");
        let empty = distribution_shift(&[1.0], &[], 0.5).unwrap_err();
        assert_eq!(empty.code(), "SHIFT_SAMPLE_EMPTY");
        let bad = distribution_shift(&[1.0, f64::NAN], &[1.0], 0.5).unwrap_err();
        assert_eq!(bad.code(), "SIZING_INPUT_INVALID");
        let bad = distribution_shift(&[1.0], &[1.0], f64::NAN).unwrap_err();
        assert_eq!(bad.code(), "SIZING_INPUT_INVALID");
    }
}
