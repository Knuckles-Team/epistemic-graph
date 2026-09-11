// CONCEPT:EG-KG.compute.hmm-regime-detection — HMM Regime Detection Engine
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

struct HmmParameters {
    means: Vec<f64>,
    stds: Vec<f64>,
    initial_probs: Vec<f64>,
    transition_matrix: Vec<Vec<f64>>,
}

fn initialize_parameters(observations: &[f64], n_states: usize) -> HmmParameters {
    let (t, mut sorted) = (observations.len(), observations.to_vec());
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let global_mean: f64 = observations.iter().sum::<f64>() / t as f64;
    let global_var: f64 = observations
        .iter()
        .map(|x| (x - global_mean).powi(2))
        .sum::<f64>()
        / t as f64;
    let global_std = global_var.sqrt().max(1e-8);

    let means: Vec<f64> = (0..n_states)
        .map(|i| {
            let quantile = (i as f64 + 0.5) / n_states as f64;
            let idx = (quantile * (t - 1) as f64) as usize;
            sorted[idx.min(t - 1)]
        })
        .collect();
    let stds = vec![global_std; n_states];
    let initial_probs = vec![1.0 / n_states as f64; n_states];
    let transition_matrix = vec![vec![1.0 / n_states as f64; n_states]; n_states];

    HmmParameters {
        means,
        stds,
        initial_probs,
        transition_matrix,
    }
}

fn forward_log_probabilities(observations: &[f64], params: &HmmParameters) -> Vec<Vec<f64>> {
    let (t, n_states) = (observations.len(), params.means.len());
    let mut log_alpha = vec![vec![0.0_f64; n_states]; t];
    for s in 0..n_states {
        log_alpha[0][s] = params.initial_probs[s].max(1e-300).ln()
            + normal_pdf(observations[0], params.means[s], params.stds[s])
                .max(1e-300)
                .ln();
    }
    for tt in 1..t {
        for j in 0..n_states {
            let emission = normal_pdf(observations[tt], params.means[j], params.stds[j])
                .max(1e-300)
                .ln();
            let trans_probs: Vec<f64> = (0..n_states)
                .map(|i| log_alpha[tt - 1][i] + params.transition_matrix[i][j].max(1e-300).ln())
                .collect();
            log_alpha[tt][j] = log_sum_exp(&trans_probs) + emission;
        }
    }
    log_alpha
}

fn backward_log_probabilities(observations: &[f64], params: &HmmParameters) -> Vec<Vec<f64>> {
    let (t, n_states) = (observations.len(), params.means.len());
    let mut log_beta = vec![vec![0.0_f64; n_states]; t];
    for tt in (0..t - 1).rev() {
        for i in 0..n_states {
            let vals: Vec<f64> = (0..n_states)
                .map(|j| {
                    params.transition_matrix[i][j].max(1e-300).ln()
                        + normal_pdf(observations[tt + 1], params.means[j], params.stds[j])
                            .max(1e-300)
                            .ln()
                        + log_beta[tt + 1][j]
                })
                .collect();
            log_beta[tt][i] = log_sum_exp(&vals);
        }
    }
    log_beta
}

fn posterior_probabilities(log_alpha: &[Vec<f64>], log_beta: &[Vec<f64>]) -> Vec<Vec<f64>> {
    let (t, n_states) = (log_alpha.len(), log_alpha[0].len());
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
    gamma
}

fn update_initial_probabilities(gamma: &[Vec<f64>], params: &mut HmmParameters) {
    let gamma_sum_0: f64 = gamma[0].iter().sum();
    for s in 0..params.initial_probs.len() {
        params.initial_probs[s] = (gamma[0][s] / gamma_sum_0).max(1e-8);
    }
}

fn update_transition_matrix(
    observations: &[f64],
    log_alpha: &[Vec<f64>],
    log_beta: &[Vec<f64>],
    log_likelihood: f64,
    params: &mut HmmParameters,
) {
    let (t, n_states) = (observations.len(), params.means.len());
    for i in 0..n_states {
        let mut row_sum = 0.0;
        for j in 0..n_states {
            let mut xi_sum = 0.0;
            for tt in 0..t - 1 {
                let xi_val = (log_alpha[tt][i]
                    + params.transition_matrix[i][j].max(1e-300).ln()
                    + normal_pdf(observations[tt + 1], params.means[j], params.stds[j])
                        .max(1e-300)
                        .ln()
                    + log_beta[tt + 1][j]
                    - log_likelihood)
                    .exp();
                xi_sum += xi_val;
            }
            params.transition_matrix[i][j] = xi_sum.max(1e-8);
            row_sum += params.transition_matrix[i][j];
        }
        for j in 0..n_states {
            params.transition_matrix[i][j] /= row_sum.max(1e-8);
        }
    }
}

fn update_emissions(observations: &[f64], gamma: &[Vec<f64>], params: &mut HmmParameters) {
    let n_states = params.means.len();
    for s in 0..n_states {
        let gamma_s_sum: f64 = gamma.iter().map(|g| g[s]).sum();
        if gamma_s_sum > 1e-8 {
            params.means[s] = gamma
                .iter()
                .enumerate()
                .map(|(tt, g)| g[s] * observations[tt])
                .sum::<f64>()
                / gamma_s_sum;

            let var: f64 = gamma
                .iter()
                .enumerate()
                .map(|(tt, g)| g[s] * (observations[tt] - params.means[s]).powi(2))
                .sum::<f64>()
                / gamma_s_sum;
            params.stds[s] = var.sqrt().max(1e-8);
        }
    }
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

    let mut params = initialize_parameters(observations, n_states);
    let mut prev_ll = f64::NEG_INFINITY;
    let mut n_iter = 0;

    for iter in 0..max_iter {
        n_iter = iter + 1;

        // E-step: Forward-backward
        let log_alpha = forward_log_probabilities(observations, &params);
        let log_beta = backward_log_probabilities(observations, &params);

        // Log-likelihood
        let ll_parts: Vec<f64> = (0..n_states).map(|s| log_alpha[t - 1][s]).collect();
        let ll = log_sum_exp(&ll_parts);

        if (ll - prev_ll).abs() < tol {
            break;
        }
        prev_ll = ll;

        // Compute gamma (state posteriors) and xi (transition posteriors)
        let gamma = posterior_probabilities(&log_alpha, &log_beta);

        // M-step: update parameters
        update_initial_probabilities(&gamma, &mut params);
        update_transition_matrix(observations, &log_alpha, &log_beta, ll, &mut params);
        update_emissions(observations, &gamma, &mut params);
    }

    // Viterbi decoding for most likely state sequence
    let states = viterbi(
        observations,
        &params.means,
        &params.stds,
        &params.initial_probs,
        &params.transition_matrix,
    );

    RegimeResult {
        states,
        means: params.means,
        stds: params.stds,
        transition_matrix: params.transition_matrix,
        initial_probs: params.initial_probs,
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
            + normal_pdf(observations[0], means[s], stds[s])
                .max(1e-300)
                .ln();
    }

    for tt in 1..t {
        for j in 0..n {
            let emission = normal_pdf(observations[tt], means[j], stds[j])
                .max(1e-300)
                .ln();
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
        // Bull regime (50 obs) then bear regime (50 obs).
        let mut obs = vec![0.01; 50];
        obs.extend(std::iter::repeat_n(-0.02, 50));

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

    #[test]
    fn test_regime_parameters_remain_normalized() {
        let observations = [0.02, 0.018, 0.021, -0.03, -0.028, -0.031];
        let result = detect_regimes(&observations, 2, 100, 1e-8);

        assert!(result.means.iter().all(|mean| mean.is_finite()));
        assert!(result.stds.iter().all(|std| *std >= 1e-8));
        assert!((result.initial_probs.iter().sum::<f64>() - 1.0).abs() < 1e-6);
        for row in &result.transition_matrix {
            assert!((row.iter().sum::<f64>() - 1.0).abs() < 1e-6);
        }
    }
}
