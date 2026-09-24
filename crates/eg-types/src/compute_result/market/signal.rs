//! Indicator specifications, the per-series trend signal and its flip records
//! (EH-414, EH-415).

use serde::{Deserialize, Serialize};

use super::bars::{BarRecord, Timeframe};

/// The candles a trailing-trend line is computed over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CandleBasis {
    Raw,
    HeikinAshi,
}

/// A deterministic incremental indicator and its bound parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum IndicatorKind {
    /// Wilder average true range, seeded by the mean of the first `period` ranges.
    Atr { period: u32 },
    /// ATR trailing trend line (SuperTrend): `multiplier_milli` = 3000 is 3.0.
    SuperTrend {
        atr_period: u32,
        multiplier_milli: u32,
        basis: CandleBasis,
    },
    /// Simple moving average of closes (`period` 200 on weekly bars is the 200W SMA).
    Sma { period: u32 },
    /// Exponential moving average of closes, seeded by the SMA of the first `period`.
    Ema { period: u32 },
    /// The 20/21 band: SMA(`sma_period`) and EMA(`ema_period`) of closes (weekly).
    Band { sma_period: u32, ema_period: u32 },
    /// Heikin-Ashi candles.
    HeikinAshi,
}

/// A versioned indicator: a formula change is a new `version`, never an edit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct IndicatorSpec {
    pub version: u32,
    pub kind: IndicatorKind,
}

/// A trend direction; only ever present when computable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum Direction {
    Bullish,
    Bearish,
}

/// The data status, reported separately from any direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DataStatus {
    /// Fewer final bars than the indicator needs.
    Warming,
    Valid,
    /// The last final bar is older than the caller's staleness bound.
    Stale,
    /// No final bar at all.
    Unavailable,
}

/// One indicator output, in milli-ticks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum IndicatorValue {
    Warming,
    Line {
        value: i64,
    },
    Band {
        sma: i64,
        ema: i64,
    },
    Trail {
        line: i64,
        atr: i64,
        direction: Direction,
    },
    Candle {
        open: i64,
        high: i64,
        low: i64,
        close: i64,
    },
}

/// The indicator value after one closed bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct IndicatorPoint {
    pub open_time: i64,
    pub close_time: i64,
    pub value: IndicatorValue,
}

/// What a bar series is: one listing, price basis, timeframe and calendar.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SeriesIdentity {
    pub listing_id: String,
    pub price_basis: String,
    pub timeframe: Timeframe,
    pub calendar_id: String,
}

/// `listing + price basis + timeframe/calendar + indicator version + parameter hash`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SignalKey {
    pub series: SeriesIdentity,
    /// e.g. `super_trend@1`.
    pub indicator_version: String,
    /// `sha256:<hex>` of the canonical parameter encoding.
    pub param_hash: String,
    /// `sha256:<hex>` of the whole key; what flip event ids are derived from.
    pub digest: String,
}

/// The exact state of the trailing-trend kernel after the last final bar, so the
/// next bar advances it in O(1). All prices in milli-ticks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SuperTrendCheckpoint {
    pub bars: u64,
    pub true_range_sum: i64,
    pub atr: Option<i64>,
    pub prev_close: Option<i64>,
    pub upper: Option<i64>,
    pub lower: Option<i64>,
    pub direction: Option<Direction>,
    /// Previous Heikin-Ashi open and close, for the Heikin-Ashi basis.
    pub ha_open: Option<i64>,
    pub ha_close: Option<i64>,
}

/// The latest state of one signal. A projection of the final bars: rebuilt by
/// replay, advanced one final bar at a time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SignalState {
    pub key: SignalKey,
    pub spec: IndicatorSpec,
    pub direction: Option<Direction>,
    pub data_status: DataStatus,
    /// The trailing line, milli-ticks.
    pub line: Option<i64>,
    pub atr: Option<i64>,
    /// Open time of the last final bar consumed.
    pub last_bar_open: Option<i64>,
    /// Close time of the last final bar consumed.
    pub last_bar_close: Option<i64>,
    /// Close of the last final bar, ticks.
    pub last_close: Option<i64>,
    /// Bar close of the last witnessed flip; never invented for the first state.
    pub last_flip_at: Option<i64>,
    /// The close (ticks) the last flip happened at: return-since-flip reference.
    pub flip_reference_price: Option<i64>,
    /// `sha256:<hex>` over the `(open_time, revision)` of every bar consumed.
    pub source_revision: String,
    pub kernel: SuperTrendCheckpoint,
}

/// One flip, at the close of one final bar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TrendFlip {
    /// `sha256:<hex>` over the key digest and the bar close: the idempotency key.
    pub event_id: String,
    pub key_digest: String,
    pub from: Direction,
    pub to: Direction,
    pub bar_open: i64,
    /// The bar's close time: when the flip took effect.
    pub effective_at: i64,
    /// When the flipping bar version became knowable.
    pub observed_at: i64,
    /// The bar close, ticks.
    pub price: i64,
    /// The trailing line after the flip, milli-ticks.
    pub line: i64,
    pub bar_revision: u32,
}

/// Whether a flip record asserts a flip or withdraws a prior one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FlipRecordStatus {
    Emitted,
    /// Revised data removed the flip; `revises` names the record withdrawn.
    Retracted,
}

/// An append-only flip record. A revision never rewrites a record: it appends one
/// that `revises` the prior record id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FlipRecord {
    /// `sha256:<hex>` over the record's content.
    pub record_id: String,
    pub status: FlipRecordStatus,
    pub flip: TrendFlip,
    pub revises: Option<String>,
    /// The `known_at` of the arrival that produced this record.
    pub recorded_at: i64,
}

/// Replay a series' bar arrivals (every version, in any order) through one
/// trailing-trend signal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SignalReplayRequest {
    pub series: SeriesIdentity,
    pub spec: IndicatorSpec,
    pub records: Vec<BarRecord>,
    /// Replay only versions known at or before this time (point in time).
    #[serde(default)]
    pub as_of: Option<i64>,
    /// With `as_of`: a last bar closing longer ago than this is `stale`.
    #[serde(default)]
    pub stale_after: Option<i64>,
}

/// The replayed state, the append-only flip records and the flips standing now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SignalReplay {
    pub state: SignalState,
    pub records: Vec<FlipRecord>,
    pub current: Vec<TrendFlip>,
}

/// A state advanced by new final bars, and the flips those bars produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SignalAdvanced {
    pub state: SignalState,
    pub flips: Vec<TrendFlip>,
}
