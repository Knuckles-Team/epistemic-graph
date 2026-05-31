// CONCEPT:KG-2.20d — Signal Generation Engine
//
// Rolling Z-score, alpha combination, signal decay, and cross-sectional ranking.
// Core signal primitives for quantitative alpha research.

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SignalOutput {
    pub values: Vec<f64>,
    pub signal_name: String,
}

/// Compute rolling Z-score: (x - rolling_mean) / rolling_std
pub fn rolling_zscore(values: &[f64], window: usize) -> Vec<f64> {
    let n = values.len();
    if n == 0 || window == 0 {
        return vec![];
    }
    let mut result = vec![f64::NAN; n];

    for i in (window - 1)..n {
        let slice = &values[(i + 1 - window)..=i];
        let mean: f64 = slice.iter().sum::<f64>() / window as f64;
        let var: f64 = slice.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / window as f64;
        let std = var.sqrt();
        if std > 1e-12 {
            result[i] = (values[i] - mean) / std;
        } else {
            result[i] = 0.0;
        }
    }
    result
}

/// Exponential weighted moving average (EWMA) signal.
pub fn ewma_signal(values: &[f64], span: usize) -> Vec<f64> {
    if values.is_empty() || span == 0 {
        return vec![];
    }
    let alpha = 2.0 / (span as f64 + 1.0);
    let mut result = vec![0.0; values.len()];
    result[0] = values[0];
    for i in 1..values.len() {
        result[i] = alpha * values[i] + (1.0 - alpha) * result[i - 1];
    }
    result
}

/// Signal decay — exponential decay applied to a signal.
pub fn signal_decay(signal: &[f64], half_life: f64) -> Vec<f64> {
    if signal.is_empty() {
        return vec![];
    }
    let decay_factor = (-0.693 / half_life).exp(); // ln(2) ≈ 0.693
    let n = signal.len();
    let mut decayed = vec![0.0; n];
    decayed[n - 1] = signal[n - 1];

    for i in (0..n - 1).rev() {
        decayed[i] = signal[i] + decay_factor * decayed[i + 1];
    }
    decayed
}

/// Alpha combination — weighted linear combination of multiple alpha signals.
pub fn combine_alphas(
    signals: &[Vec<f64>],
    weights: &[f64],
) -> Vec<f64> {
    if signals.is_empty() || weights.is_empty() {
        return vec![];
    }
    let n = signals[0].len();
    let mut combined = vec![0.0; n];

    for (signal, &weight) in signals.iter().zip(weights.iter()) {
        for i in 0..n.min(signal.len()) {
            let val = signal[i];
            if val.is_finite() {
                combined[i] += weight * val;
            }
        }
    }
    combined
}

/// Cross-sectional rank — rank values at each time step.
/// Returns ranks as percentiles (0.0 to 1.0).
pub fn cross_sectional_rank(
    cross_section: &[Vec<f64>],
) -> Vec<Vec<f64>> {
    if cross_section.is_empty() {
        return vec![];
    }
    let n_assets = cross_section.len();
    let n_times = cross_section[0].len();
    let mut ranked = vec![vec![0.0; n_times]; n_assets];

    for t in 0..n_times {
        let mut vals: Vec<(usize, f64)> = (0..n_assets)
            .map(|a| (a, if t < cross_section[a].len() { cross_section[a][t] } else { f64::NAN }))
            .filter(|(_, v)| v.is_finite())
            .collect();

        vals.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let count = vals.len();
        for (rank, (asset_idx, _)) in vals.iter().enumerate() {
            ranked[*asset_idx][t] = if count > 1 {
                rank as f64 / (count - 1) as f64
            } else {
                0.5
            };
        }
    }
    ranked
}

/// Momentum signal — (price / price_n_periods_ago) - 1
pub fn momentum(prices: &[f64], lookback: usize) -> Vec<f64> {
    let n = prices.len();
    if n <= lookback {
        return vec![f64::NAN; n];
    }
    let mut result = vec![f64::NAN; n];
    for i in lookback..n {
        if prices[i - lookback].abs() > 1e-12 {
            result[i] = prices[i] / prices[i - lookback] - 1.0;
        }
    }
    result
}

/// Mean reversion signal — deviation from rolling mean, normalized.
pub fn mean_reversion(values: &[f64], window: usize) -> Vec<f64> {
    let zscore = rolling_zscore(values, window);
    // Negate: large positive z-score → sell signal (expect reversion)
    zscore.iter().map(|z| if z.is_finite() { -z } else { f64::NAN }).collect()
}

/// Compute Information Coefficient (IC) — correlation between signal and forward returns.
pub fn information_coefficient(signal: &[f64], forward_returns: &[f64]) -> f64 {
    let n = signal.len().min(forward_returns.len());
    if n < 2 {
        return 0.0;
    }

    // Filter out NaN pairs
    let pairs: Vec<(f64, f64)> = signal.iter()
        .zip(forward_returns.iter())
        .filter(|(&s, &r)| s.is_finite() && r.is_finite())
        .map(|(&s, &r)| (s, r))
        .collect();

    let n = pairs.len();
    if n < 2 {
        return 0.0;
    }

    let mean_s: f64 = pairs.iter().map(|(s, _)| s).sum::<f64>() / n as f64;
    let mean_r: f64 = pairs.iter().map(|(_, r)| r).sum::<f64>() / n as f64;

    let mut cov = 0.0;
    let mut var_s = 0.0;
    let mut var_r = 0.0;
    for (s, r) in &pairs {
        cov += (s - mean_s) * (r - mean_r);
        var_s += (s - mean_s).powi(2);
        var_r += (r - mean_r).powi(2);
    }

    let denom = (var_s * var_r).sqrt();
    if denom > 1e-12 {
        cov / denom
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rolling_zscore() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let result = rolling_zscore(&values, 3);
        assert_eq!(result.len(), 10);
        assert!(result[0].is_nan());
        assert!(result[2].is_finite());
    }

    #[test]
    fn test_ewma() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = ewma_signal(&values, 3);
        assert_eq!(result.len(), 5);
        assert_eq!(result[0], 1.0);
    }

    #[test]
    fn test_combine_alphas() {
        let s1 = vec![1.0, 2.0, 3.0];
        let s2 = vec![3.0, 2.0, 1.0];
        let combined = combine_alphas(&[s1, s2], &[0.6, 0.4]);
        assert_eq!(combined.len(), 3);
        assert!((combined[0] - 1.8).abs() < 1e-10); // 0.6*1 + 0.4*3
    }

    #[test]
    fn test_momentum() {
        let prices = vec![100.0, 105.0, 110.0, 108.0, 112.0];
        let result = momentum(&prices, 2);
        assert!(result[2].is_finite());
        assert!((result[2] - 0.10).abs() < 1e-10); // 110/100 - 1
    }

    #[test]
    fn test_information_coefficient() {
        let signal = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let returns = vec![0.1, 0.2, 0.3, 0.4, 0.5];
        let ic = information_coefficient(&signal, &returns);
        assert!((ic - 1.0).abs() < 1e-10); // Perfectly correlated
    }
}
