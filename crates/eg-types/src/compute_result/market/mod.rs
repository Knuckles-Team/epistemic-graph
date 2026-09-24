//! Market bars, trend signals and their evidence records (EH-413..EH-418).
//!
//! The wire bodies of the `FinanceMarket` method: typed OHLCV bars over the
//! time-series store, versioned indicator specifications, the per-series signal
//! state with its trend-flip records, the Decide-calibrated flip confidence and
//! the backtest-run provenance record. Prices are integer ticks and every
//! indicator value is an integer count of milli-ticks, so each result is
//! bit-identical on every build target. Everything here is informational only:
//! no value in this module authorises an order.

pub mod bars;
pub mod evidence;
pub mod op;
pub mod scan;
pub mod signal;

pub use bars::{
    BarRecord, BarStatus, EarlyClose, ExchangeCalendar, FinalityFilter, SeriesPoint, Timeframe,
    TradingCalendar, UtcOffsetSpan,
};
pub use evidence::{
    BacktestRun, BacktestRunDraft, BacktestValidation, CostModel, DataRevisionRef, FillRule,
    FlipAbstainReason, FlipConfidence, FlipConfidenceRequest, FlipFeatures, FlipOutcomeSample,
    TradeFill, UniverseMember, ValidationInputs,
};
pub use op::FinanceMarketOp;
pub use scan::{ScanCounts, ScanFilter, ScanPage, ScanRequest, ScanRow};
pub use signal::{
    CandleBasis, DataStatus, Direction, FlipRecord, FlipRecordStatus, IndicatorKind,
    IndicatorPoint, IndicatorSpec, IndicatorValue, SeriesIdentity, SignalKey, SignalReplay,
    SignalReplayRequest, SignalState, SuperTrendCheckpoint, TrendFlip,
};

/// Indicator values are integer milli-ticks: one price tick is this many units.
pub const MILLI_TICKS_PER_TICK: i64 = 1_000;
