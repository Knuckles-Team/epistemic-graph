//! The operations of the `FinanceMarket` method. Every one is pure compute over
//! what the request carries: no store, no clock, no order authority.

use serde::{Deserialize, Serialize};

use super::bars::{BarRecord, FinalityFilter, SeriesPoint, Timeframe, TradingCalendar};
use super::evidence::{BacktestRunDraft, FlipConfidenceRequest};
use super::scan::ScanRequest;
use super::signal::{IndicatorSpec, SignalReplayRequest, SignalState};

/// One market-signal operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FinanceMarketOp {
    /// Bar records as time-series store points (the `TsAppend` layout).
    EncodePoints { records: Vec<BarRecord> },
    /// The latest version of every bar known as of `as_of` (every version
    /// when absent), ordered by open time, over `records` together with
    /// time-series store `points` decoded and validated in the bar layout.
    Resolve {
        #[serde(default)]
        records: Vec<BarRecord>,
        #[serde(default)]
        points: Vec<SeriesPoint>,
        #[serde(default)]
        as_of: Option<i64>,
        finality: FinalityFilter,
    },
    /// Resolved bars rolled up to a coarser calendar timeframe; a rolled bar is
    /// final only when every part is final and `watermark` has passed its close.
    Rollup {
        bars: Vec<BarRecord>,
        calendar: TradingCalendar,
        timeframe: Timeframe,
        watermark: i64,
    },
    /// One indicator over resolved final bars, one value per bar.
    Indicators {
        bars: Vec<BarRecord>,
        spec: IndicatorSpec,
    },
    /// Replay every bar version of one series through a trailing-trend signal.
    SignalReplay { request: SignalReplayRequest },
    /// Advance a signal state by new final bars after its last one, in O(1) each.
    SignalAdvance {
        state: SignalState,
        bars: Vec<BarRecord>,
    },
    /// The latest-state scanner view over many signal states.
    SignalScan { request: ScanRequest },
    /// Decide-calibrated follow-through for one flip, or an abstention.
    FlipConfidence { request: FlipConfidenceRequest },
    /// Validate a backtest run and seal it as a content-addressed record.
    BacktestRun { draft: BacktestRunDraft },
}
