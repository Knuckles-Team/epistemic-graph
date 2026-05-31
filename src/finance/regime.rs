// CONCEPT:KG-2.20c — HMM Regime Detection Engine
//
// Hidden Markov Model for market regime detection.
// Implements Baum-Welch (EM) for parameter estimation and Viterbi for decoding.
// Replaces Python hmmlearn.

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RegimeResult {
    /// Most likely state sequence (Viterbi path)
    pub states: Vec<usize>,
    /// State means (emission parameters)
    pub means: Vec<f64>,
    /// State standard deviations
    pub stds: Vec<f64>,
    /// Transition matrix (n_states x n_states)
    pub transition_matrix: Vec<Vec<f64>>,
    /// Initial state probabilities
    pub initial_probs: Vec<f64>,
    /// Log-likelihood of the final model
    pub log_likelihood: f64,
    /// Number of EM iterations until convergence
    pub n_iterations: usize,
}

/// Normal PDF: N(x | μ, σ)
fn normal_pdf(x: f64, mean: f64, std: f64) -> f64 {
    let z = (x - mean) / std;
    (-0.5 * z * z).exp() / (std * (2.0 * std::f64::consts::PI).sqrt())
}

/// Log-sum-exp for numerical stability.
fn log_sum_exp(values: &[f64]) -> f64 {
    let max_val = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    if max_val.is_infinite() {
        return f64::NEG_INFINITY;
    }
    max_val + values.iter().map(|v| (v - max_val).exp()).sum::<f64>().ln()
}

/// Detect market regimes using a Gaussian HMM with Baum-Welch EM.
///
/// # Arguments
/// * `observations` — time series of returns
/// * `n_states` — number of hidden states (typically 2-3: bull/bear/neutral)
/// * `max_iter` — maximum EM iterations
/// * `tol` — convergence tolerance on log-likelihood
pub fn detect_regimes(
    observations: &[f64],
    n_states: usize,
    max_iter: usize,
    tol: f64,
) -> RegimeResult {
    let t = observations.len();
    if t == 0 || n_states == 0 {
        return RegimeResult {
            states: vec![],
            means: vec![],
            stds: vec![],
            transition_matrix: vec![],
            initial_probs: vec![],
            log_likelihood: 0.0,
            n_iterations: 0,
        };
    }

    // Initialize parameters using data quantiles
    let mut sorted = observations.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let global_mean: f64 = observations.iter().sum::<f64>() / t as f64;
    let global_var: f64 = observations.iter().map(|x| (x - global_mean).powi(2)).sum::<f64>() / t as f64;
    let global_std = global_var.sqrt().max(1e-8);

    let mut means: Vec<f64> = (0..n_states)
        .map(|i| {
            let quantile = (i as f64 + 0.5) / n_states as f64;
            let idx = (quantile * (t - 1) as f64) as usize;
            sorted[idx.min(t - 1)]
        })
        .collect();

    let mut stds: Vec<f64> = vec![global_std; n_states];

    // Uniform initial/transition probabilities
    let mut pi: Vec<f64> = vec![1.0 / n_states as f64; n_states];
    let mut trans: Vec<Vec<f64>> = vec![vec![1.0 / n_states as f64; n_states]; n_states];

    let mut prev_ll = f64::NEG_INFINITY;
    let mut n_iter = 0;

    for iter in 0..max_iter {
        n_iter = iter + 1;

        // E-step: Forward-backward
        // Forward pass (log space)
        let mut log_alpha = vec![vec![0.0_f64; n_states]; t];
        for s in 0..n_states {
            log_alpha[0][s] = pi[s].max(1e-300).ln() + normal_pdf(observations[0], means[s], stds[s]).max(1e-300).ln();
        }
        for tt in 1..t {
            for j in 0..n_states {
                let emission = normal_pdf(observations[tt], means[j], stds[j]).max(1e-300).ln();
                let trans_probs: Vec<f64> = (0..n_states)
                    .map(|i| log_alpha[tt - 1][i] + trans[i][j].max(1e-300).ln())
                    .collect();
                log_alpha[tt][j] = log_sum_exp(&trans_probs) + emission;
            }
        }

        // Backward pass (log space)
        let mut log_beta = vec![vec![0.0_f64; n_states]; t];
        // beta[T-1] = 0 in log space (i.e., beta = 1)
        for tt in (0..t - 1).rev() {
            for i in 0..n_states {
                let vals: Vec<f64> = (0..n_states)
                    .map(|j| {
                        trans[i][j].max(1e-300).ln()
                            + normal_pdf(observations[tt + 1], means[j], stds[j]).max(1e-300).ln()
                            + log_beta[tt + 1][j]
                    })
                    .collect();
                log_beta[tt][i] = log_sum_exp(&vals);
            }
        }

        // Log-likelihood
        let ll_parts: Vec<f64> = (0..n_states).map(|s| log_alpha[t - 1][s]).collect();
        let ll = log_sum_exp(&ll_parts);

        if (ll - prev_ll).abs() < tol {
            break;
        }
        prev_ll = ll;

        // Compute gamma (state posteriors) and xi (transition posteriors)
        let mut gamma = vec![vec![0.0_f64; n_states]; t];
        for tt in 0..t {
            let vals: Vec<f64> = (0..n_states)
                .map(|s| log_alpha[tt][s] + log_beta[tt][s])
                .collect();
            let norm = log_sum_exp(&vals);
            for s in 0..n_states {
                gamma[tt][s] = (log_alpha[tt][s] + log_beta[tt][s] - norm).exp();
            }
        }

        // M-step: update parameters
        // Initial probabilities
        let gamma_sum_0: f64 = gamma[0].iter().sum();
        for s in 0..n_states {
            pi[s] = (gamma[0][s] / gamma_sum_0).max(1e-8);
        }

        // Transition matrix
        for i in 0..n_states {
            let mut row_sum = 0.0;
            for j in 0..n_states {
                let mut xi_sum = 0.0;
                for tt in 0..t - 1 {
                    let xi_val = (log_alpha[tt][i]
                        + trans[i][j].max(1e-300).ln()
                        + normal_pdf(observations[tt + 1], means[j], stds[j]).max(1e-300).ln()
                        + log_beta[tt + 1][j]
                        - ll)
                        .exp();
                    xi_sum += xi_val;
                }
                trans[i][j] = xi_sum.max(1e-8);
                row_sum += trans[i][j];
            }
            for j in 0..n_states {
                trans[i][j] /= row_sum.max(1e-8);
            }
        }

        // Emission parameters
        for s in 0..n_states {
            let gamma_s_sum: f64 = gamma.iter().map(|g| g[s]).sum();
            if gamma_s_sum > 1e-8 {
                means[s] = gamma.iter().enumerate()
                    .map(|(tt, g)| g[s] * observations[tt])
                    .sum::<f64>() / gamma_s_sum;

                let var: f64 = gamma.iter().enumerate()
                    .map(|(tt, g)| g[s] * (observations[tt] - means[s]).powi(2))
                    .sum::<f64>() / gamma_s_sum;
                stds[s] = var.sqrt().max(1e-8);
            }
        }
    }

    // Viterbi decoding for most likely state sequence
    let states = viterbi(observations, &means, &stds, &pi, &trans);

    RegimeResult {
        states,
        means,
        stds,
        transition_matrix: trans,
        initial_probs: pi,
        log_likelihood: prev_ll,
        n_iterations: n_iter,
    }
}

/// Viterbi algorithm — find the most likely hidden state sequence.
fn viterbi(
    observations: &[f64],
    means: &[f64],
    stds: &[f64],
    pi: &[f64],
    trans: &[Vec<f64>],
) -> Vec<usize> {
    let t = observations.len();
    let n = means.len();
    if t == 0 {
        return vec![];
    }

    let mut v = vec![vec![0.0_f64; n]; t];
    let mut backtrack = vec![vec![0usize; n]; t];

    for s in 0..n {
        v[0][s] = pi[s].max(1e-300).ln()
            + normal_pdf(observations[0], means[s], stds[s]).max(1e-300).ln();
    }

    for tt in 1..t {
        for j in 0..n {
            let emission = normal_pdf(observations[tt], means[j], stds[j]).max(1e-300).ln();
            let mut best_val = f64::NEG_INFINITY;
            let mut best_state = 0;
            for i in 0..n {
                let score = v[tt - 1][i] + trans[i][j].max(1e-300).ln();
                if score > best_val {
                    best_val = score;
                    best_state = i;
                }
            }
            v[tt][j] = best_val + emission;
            backtrack[tt][j] = best_state;
        }
    }

    // Backtrack
    let mut states = vec![0usize; t];
    let mut best_last = 0;
    let mut best_val = f64::NEG_INFINITY;
    for s in 0..n {
        if v[t - 1][s] > best_val {
            best_val = v[t - 1][s];
            best_last = s;
        }
    }
    states[t - 1] = best_last;
    for tt in (0..t - 1).rev() {
        states[tt] = backtrack[tt + 1][states[tt + 1]];
    }

    states
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regime_detection_two_states() {
        // Generate synthetic two-regime data
        let mut obs = Vec::new();
        // Bull regime
        for _ in 0..50 {
            obs.push(0.01);
        }
        // Bear regime
        for _ in 0..50 {
            obs.push(-0.02);
        }

        let result = detect_regimes(&obs, 2, 100, 1e-6);
        assert_eq!(result.states.len(), 100);
        assert_eq!(result.means.len(), 2);
        assert_eq!(result.transition_matrix.len(), 2);
    }

    #[test]
    fn test_empty_observations() {
        let result = detect_regimes(&[], 2, 100, 1e-6);
        assert!(result.states.is_empty());
    }
}
