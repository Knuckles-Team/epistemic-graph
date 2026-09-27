//! Evidence about signals: Decide-calibrated flip confidence (EH-417) and the
//! backtest-run provenance record (EH-418). Neither authorises an order.

use serde::{Deserialize, Serialize};

use super::bars::Timeframe;
use super::signal::{DataStatus, Direction};

/// The features a flip is conditioned on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FlipFeatures {
    /// The higher timeframe's direction agrees with the flip.
    pub timeframe_agreement: bool,
    /// The close is above the 200-week SMA.
    pub above_200w_sma: bool,
    /// A regime label (e.g. from `FinanceDetectRegimes`).
    pub regime: u32,
}

/// One historical flip and whether the move followed through over the horizon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FlipOutcomeSample {
    pub effective_at: i64,
    pub direction: Direction,
    pub features: FlipFeatures,
    pub followed_through: bool,
}

/// Calibrate one candidate flip's follow-through at one horizon against the
/// history of the same indicator version, timeframe and asset class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FlipConfidenceRequest {
    pub indicator_version: String,
    pub timeframe: Timeframe,
    pub asset_class: String,
    pub horizon_bars: u32,
    pub direction: Direction,
    pub features: FlipFeatures,
    /// The candidate signal's data status; anything but `valid` abstains.
    pub data_status: DataStatus,
    pub history: Vec<FlipOutcomeSample>,
    /// Conformal miscoverage in permille (100 = 0.1).
    pub alpha_permille: u32,
    /// Policy minimum of historical flips.
    pub n_min: u32,
}

/// Why no confidence was given.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FlipAbstainReason {
    /// The signal is warming, stale or unavailable.
    DataNotValid { data_status: DataStatus },
    /// Fewer historical flips than the policy (or the conformal level) needs.
    InsufficientHistory { n: u64, n_min: u64 },
    /// Too few historical flips in the candidate's regime.
    RegimeUnsupported { regime: u32, n: u64, n_min: u64 },
    /// The recent half of the history no longer resembles the earlier half.
    Drift {
        earlier_lower: f64,
        earlier_upper: f64,
        recent_rate: f64,
    },
    /// The conformal set holds both outcomes (or neither): no singleton claim.
    AmbiguousSet,
}

/// A calibrated follow-through claim, or a typed abstention. There is no third arm.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FlipConfidence {
    Calibrated {
        horizon_bars: u32,
        /// Smoothed follow-through rate of the candidate's feature cell.
        probability: f64,
        /// The singleton conformal set: `true` = follows through.
        follows_through: bool,
        alpha_permille: u32,
        n_calibration: u64,
    },
    Abstained {
        horizon_bars: u32,
        reason: FlipAbstainReason,
    },
}

/// A point-in-time universe member: tradable in `[from, until)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct UniverseMember {
    pub listing_id: String,
    pub from: i64,
    #[serde(default)]
    pub until: Option<i64>,
}

/// One bar series' data as the backtest read it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DataRevisionRef {
    pub series_id: String,
    /// The `source_revision` digest of the bars read.
    pub source_revision: String,
    /// Every bar version read was known at or before this time.
    pub known_as_of: i64,
}

/// Costs charged on every fill, in basis points of notional.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CostModel {
    pub fee_bps: u32,
    pub slippage_bps: u32,
}

/// When a signal's fill happens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FillRule {
    /// The open of the first bar after the signal is known.
    NextBarOpen,
    /// The first price actually available after the signal is known.
    FirstPriceAfterKnown,
}

/// One simulated fill.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TradeFill {
    pub listing_id: String,
    /// When the signal that caused this fill became knowable.
    pub known_at: i64,
    pub fill_at: i64,
    /// Ticks.
    pub fill_price: i64,
    pub direction: Direction,
}

/// The inputs of the mandatory validation outputs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ValidationInputs {
    pub n_groups: u32,
    pub n_test_groups: u32,
    pub purge_window: u32,
    pub embargo: u32,
    /// Strategy variants tried (the deflated Sharpe's trial count).
    pub n_trials: u32,
    /// Per-period performance, rows aligned with `BacktestRunDraft::returns`,
    /// columns = strategy variants. The engine derives both sides of every
    /// purged CPCV split from these same observations.
    pub performance: Vec<Vec<f64>>,
}

/// A backtest run as submitted, before its validation outputs are computed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct BacktestRunDraft {
    pub strategy: String,
    /// Digests of every signal key the run consumed.
    pub signal_keys: Vec<String>,
    pub data_revisions: Vec<DataRevisionRef>,
    pub universe: Vec<UniverseMember>,
    pub costs: CostModel,
    pub fill_rule: FillRule,
    pub fills: Vec<TradeFill>,
    /// Per-period strategy returns after costs.
    pub returns: Vec<f64>,
    pub validation: ValidationInputs,
    /// The record this run revises, if any; records are never edited.
    #[serde(default)]
    pub supersedes: Option<String>,
}

/// The mandatory validation outputs, computed by the engine's own kernels.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct BacktestValidation {
    /// Purged combinatorial CV: the number of splits and their test/train sizes.
    pub cpcv_splits: u32,
    pub cpcv_min_train: u32,
    pub observed_sharpe: f64,
    pub deflated_sharpe: f64,
    pub probability_backtest_overfit: f64,
}

/// The immutable, content-addressed backtest-run record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct BacktestRun {
    /// `sha256:<hex>` over the draft and the validation outputs.
    pub digest: String,
    pub draft: BacktestRunDraft,
    pub validation: BacktestValidation,
    /// Always true: a backtest is evidence, never an order authority.
    pub informational_only: bool,
}
