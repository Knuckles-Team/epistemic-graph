//! Signal fusion and strategic-insider models (EH-423 / AUD-30).
//!
//! The wire bodies of the `FinanceSignalModels` method. Two closed-form models that
//! agent-utilities used to compute in Python and emerald-exchange calls:
//!
//! * [`FinanceSignalModelsOp::BayesFuse`] -- sequential Bayesian fusion of
//!   directional calls, each source weighted by its measured accuracy and
//!   edge (`directional_accuracy * standalone_sharpe`), overfit sources
//!   (`pbo > max_pbo`) and edgeless ones (`sharpe <= min_sharpe`) dropped.
//! * [`FinanceSignalModelsOp::InsiderEquilibrium`] -- the Kyle insider's optimal
//!   intensity under a dynamic legal-risk hazard (Qiao & Xia,
//!   arXiv:2605.27684), its end-of-window schedule and the penalty-design
//!   comparative statics. A surveillance-design aid, not a trading tool.
//!
//! Pure compute over the request; informational only, never an order authority.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A stored per-source prior: what a backtest measured for one signal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FusionPrior {
    pub name: String,
    pub directional_accuracy: f64,
    pub standalone_sharpe: f64,
    pub pbo: f64,
}

/// `bayes_fuse`: fuse directional calls into `P(up)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct BayesFuseRequest {
    /// `P(up)` before any call.
    #[serde(default = "half")]
    pub prior: f64,
    /// Measured priors; a source is seeded only when it has an edge and is
    /// not overfit.
    #[serde(default)]
    pub priors: Vec<FusionPrior>,
    /// Each source's call: `1` up, `-1` down, `0` no call. Applied in name
    /// order, so the result does not depend on how a caller built the map.
    pub directions: BTreeMap<String, i8>,
    #[serde(default)]
    pub min_sharpe: f64,
    #[serde(default = "half")]
    pub max_pbo: f64,
    /// Weight of a source that called but has no seeded prior.
    #[serde(default = "half")]
    pub default_weight: f64,
    /// Accuracy of a source that called but has no seeded prior.
    #[serde(default = "default_accuracy")]
    pub default_accuracy: f64,
}

fn half() -> f64 {
    0.5
}

fn default_accuracy() -> f64 {
    0.55
}

/// One source as the fusion used it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FusionSource {
    pub name: String,
    pub weight: f64,
    pub accuracy: f64,
    /// Whether the source came from a measured prior (else the defaults).
    pub seeded: bool,
}

/// The fused probability of an upward move.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct BayesFusion {
    pub posterior_up: f64,
    /// Priors that survived the edge and overfit filters.
    pub seeded: u32,
    /// Every source that called, in the order applied.
    pub sources: Vec<FusionSource>,
}

/// Primitives of the continuous-time Kyle game with dynamic legal risk, in the
/// paper's normalized units.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct InsiderInputs {
    /// Std of the fundamental value (the informational edge).
    pub sigma_v: f64,
    /// Std of noise-trader flow (the cover for stealth).
    pub sigma_u: f64,
    /// `E[(v-p)^2]`; `sigma_v^2` when absent.
    #[serde(default)]
    pub gap_var: Option<f64>,
    /// Regulator effort in `[0, 1]`.
    pub enforcement: f64,
    /// How fast the detection hazard grows with activity.
    pub surveillance_kappa: f64,
    /// Expected criminal cost (fixed, hazard-scaled).
    pub criminal_penalty: f64,
    /// Civil damages as a multiple of gross profit.
    pub civil_penalty_rate: f64,
    /// Trading window length.
    pub horizon: f64,
}

/// `insider_equilibrium`: the equilibrium, its schedule and the policy verdict.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct InsiderEquilibriumRequest {
    pub inputs: InsiderInputs,
    /// Schedule samples over the window (`steps + 1` points).
    pub steps: u32,
}

/// Which lever constrains the insider at the equilibrium.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum BindingLever {
    Criminal,
    Civil,
    Enforcement,
    None,
}

/// The insider's optimum at one enforcement level.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct InsiderEquilibrium {
    /// Optimal trade per unit of mispricing gap.
    pub intensity: f64,
    /// Kyle intensity with no legal risk.
    pub baseline_intensity: f64,
    /// Market-maker price impact.
    pub kyle_lambda: f64,
    /// Endogenous prosecution hazard at the optimum, in `[0, 1]`.
    pub detection_prob: f64,
    pub expected_profit: f64,
    pub expected_penalty: f64,
    pub net_value: f64,
    /// Legal risk drove the intensity to the zero floor.
    pub suppressed: bool,
    pub binding_lever: BindingLever,
}

/// One schedule sample: enforcement decays with the remaining window.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct InsiderScheduleSample {
    pub t: f64,
    pub remaining: f64,
    pub enforcement: f64,
    pub intensity: f64,
    pub detection_prob: f64,
}

/// The penalty-design conclusion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PenaltyVerdict {
    /// Weak enforcement: civil penalties have vanishing effect.
    EnforcementGated,
    /// The criminal cost is past the suppression floor: intensity is zero.
    CriminalSuppresses,
    /// Criminal sanctions are the effective lever; civil damages only dampen.
    CriminalIsTheLever,
}

/// Comparative statics of the equilibrium intensity in the two penalties.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PenaltyPolicy {
    pub d_intensity_d_criminal: f64,
    pub d_intensity_d_civil: f64,
    /// Criminal cost that fully suppresses the insider; `None` when no
    /// finite cost does (no enforcement or no surveillance).
    pub criminal_intensity_floor: Option<f64>,
    /// Civil-only infimum of the intensity (approached, never reached).
    pub civil_only_min_intensity: f64,
    pub enforcement_gated: bool,
    pub verdict: PenaltyVerdict,
}

/// The full `insider_equilibrium` answer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct InsiderAnalysis {
    pub equilibrium: InsiderEquilibrium,
    pub schedule: Vec<InsiderScheduleSample>,
    pub policy: PenaltyPolicy,
}

/// One signal-model operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FinanceSignalModelsOp {
    /// Sequential Bayesian fusion of directional calls.
    BayesFuse { request: BayesFuseRequest },
    /// The strategic insider under dynamic legal risk.
    InsiderEquilibrium { request: InsiderEquilibriumRequest },
}
