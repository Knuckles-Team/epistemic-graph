//! Results of the quantitative-finance kernels (CONCEPT:EG-KG.domains.finance-compute).

use serde::{Deserialize, Serialize};

/// A mean-variance / risk-parity / Black-Litterman portfolio.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OptimizationResult {
    pub weights: Vec<f64>,
    pub expected_return: f64,
    pub expected_volatility: f64,
    pub sharpe_ratio: f64,
    pub method: String,
}

/// Summary risk metrics of a return series.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RiskMetrics {
    pub var_95: f64,
    pub var_99: f64,
    pub cvar_95: f64,
    pub cvar_99: f64,
    pub max_drawdown: f64,
    pub volatility: f64,
    pub downside_deviation: f64,
    pub sortino_ratio: f64,
    pub calmar_ratio: f64,
}

/// A fitted Gaussian HMM regime model.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
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

/// Fill result from order matching.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct Fill {
    pub order_id: String,
    pub fill_price: f64,
    pub fill_quantity: f64,
    pub side: String,
}

/// A two-sided market-making quote.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct Quote {
    pub bid: f64,
    pub ask: f64,
    pub reservation: f64,
    pub half_spread: f64,
    /// True when an inventory / boundary cap says "withdraw, do not quote".
    pub withdraw: bool,
}

/// An exponential-kernel Hawkes process fit.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HawkesFit {
    pub mu: f64,
    pub alpha: f64,
    pub beta: f64,
    pub branching_ratio: f64,
    pub half_life_seconds: f64,
    pub log_likelihood: f64,
    pub converged: bool,
}

/// Continuous-time-Kyle surveillance estimate over a trailing book/flow window.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SurveillanceRisk {
    pub kyle_lambda: f64,
    pub informed_share: f64,
    pub detection_hazard: f64,
    pub cumulative_suspicion: f64,
    pub stealth_ratio: f64,
    pub legal_risk_score: f64,
}

/// A Bayesian posterior credible interval.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PosteriorCredibleInterval {
    pub lower: f64,
    pub upper: f64,
}

/// One purged combinatorial cross-validation split.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CvSplit {
    pub train: Vec<usize>,
    pub test: Vec<usize>,
}

/// Diebold-Mariano test of equal predictive accuracy.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DieboldMariano {
    pub statistic: f64,
    pub p_value: f64,
    pub a_better: bool,
}

/// Forensic-accounting scores for one fiscal year against the prior one.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForensicReport {
    pub m_score: f64,
    pub z_score: f64,
    pub f_score: i32,
    pub accruals_ratio: f64,
    /// Human-readable flags that crossed a threshold.
    pub flags: Vec<String>,
    pub verdict: String, // "INVESTIGATE" | "CLEAN"
}

/// Filtered states and variances of a scalar Kalman filter.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct KalmanState {
    pub states: Vec<f64>,
    pub variances: Vec<f64>,
}

/// Augmented Dickey-Fuller test result.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AdfResult {
    pub statistic: f64,
    pub used_lag: usize,
    pub n_obs: usize,
    /// Finite-sample-interpolated MacKinnon critical values (constant, no trend).
    pub crit_1pct: f64,
    pub crit_5pct: f64,
    pub crit_10pct: f64,
    /// Approximate p-value (monotone interpolation across the critical points).
    pub p_value_approx: f64,
    pub stationary_1pct: bool,
    pub stationary_5pct: bool,
    pub stationary_10pct: bool,
}

/// Calibrated Ornstein-Uhlenbeck parameters.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OuParams {
    pub theta: f64,     // mean-reversion rate
    pub mu: f64,        // long-run mean
    pub sigma: f64,     // instantaneous volatility
    pub half_life: f64, // ln(2)/theta
    pub sigma_eq: f64,  // equilibrium std σ/√(2θ)
}

/// MFPT-optimal Ornstein-Uhlenbeck entry/exit thresholds.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OuThresholds {
    pub entry_long: f64,  // enter long below this (μ − z·σ_eq)
    pub entry_short: f64, // enter short above this (μ + z·σ_eq)
    pub exit: f64,        // exit at the mean
    pub z: f64,           // optimal entry deviation in σ_eq units
    pub expected_return_per_unit_time: f64,
}

/// Queue-imbalance skew and per-side fill times.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct QueueSignal {
    pub skew: Vec<f64>,
    pub bid_fill_time: Vec<f64>,
    pub ask_fill_time: Vec<f64>,
}

/// Rolling spread z-score and its mean-reversion signal.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SpreadReversion {
    pub zscore: Vec<f64>,
    pub signal: Vec<f64>,
}

/// Conviction gate over N signal strengths.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ConvergenceGate {
    pub agree: usize,
    pub total: usize,
    pub fraction: f64,
    pub direction: i32, // +1 up, -1 down, 0 none
    pub pass: bool,
}

/// A SABR smile calibration with `beta` fixed.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SabrFit {
    pub alpha: f64,
    pub beta: f64,
    pub rho: f64,
    pub nu: f64,
    pub rmse: f64,
    pub converged: bool,
}
