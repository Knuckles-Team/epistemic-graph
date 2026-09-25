// CONCEPT:EG-KG.compute.hmm-regime-detection — HMM Regime Detection Engine
//
// Hidden Markov Model for market regime detection.
// Implements Baum-Welch (EM) for parameter estimation and Viterbi for decoding.
// Replaces Python hmmlearn.

pub use eg_types::compute_result::finance::RegimeResult;

/// Detect regimes through the generic Gaussian HMM series kernel.
pub fn detect_regimes(
    observations: &[f64],
    n_states: usize,
    max_iter: usize,
    tol: f64,
) -> RegimeResult {
    let model = eg_numeric::series::regime::detect(observations, n_states, max_iter, tol);
    RegimeResult {
        states: model.states,
        means: model.means,
        stds: model.stds,
        transition_matrix: model.transition_matrix,
        initial_probs: model.initial_probs,
        log_likelihood: model.log_likelihood,
        n_iterations: model.n_iterations,
    }
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
