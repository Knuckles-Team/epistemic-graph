//! The typed OHLCV bar-series contract (EH-413).
//!
//! A bar is identified by its series and `open_time`. Bars are appended, never
//! rewritten: a correction is a new record of the same `open_time` with a higher
//! `revision`, and `known_at` records when that version became knowable, so any
//! earlier view can be reconstructed as of a point in time. All timestamps are
//! nanoseconds since the Unix epoch, UTC.

use serde::{Deserialize, Serialize};

/// Whether a bar's period has closed and its values are settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum BarStatus {
    /// The period is still open (or its data incomplete); a preview only.
    Provisional,
    /// The period has closed; signals confirm only on final bars.
    Final,
}

/// One version of one OHLCV bar. Prices are integer ticks of the series' tick
/// size; volume is integer units of the series' volume step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct BarRecord {
    pub open_time: i64,
    pub close_time: i64,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
    pub volume: i64,
    pub status: BarStatus,
    /// 0 for the first version; each correction is strictly higher.
    pub revision: u32,
    /// When this version became knowable (a final bar never before its close).
    pub known_at: i64,
}

/// A bar width. Minutes and hours align to the calendar's day (UTC midnight, or
/// the session open); day, week and month are calendar periods.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "unit", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum Timeframe {
    Minutes {
        n: u32,
    },
    Hours {
        n: u32,
    },
    Day,
    /// ISO week, Monday first.
    Week,
    Month,
}

/// A UTC offset in force from `from` (UTC nanoseconds) until the next span.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct UtcOffsetSpan {
    pub from: i64,
    pub offset_minutes: i16,
}

/// A session that closes early on one local day.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EarlyClose {
    /// Local civil day, as days since 1970-01-01.
    pub day: i32,
    pub close_minute: u16,
}

/// An exchange session calendar, entirely data: offsets (including daylight
/// saving changes) as spans, one regular session, trading weekdays, holidays and
/// early closes. No time-zone database is consulted, so the calendar a series
/// was built with is exactly reproducible.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ExchangeCalendar {
    pub id: String,
    /// Ascending by `from`; the first span applies before its own `from` too.
    pub offsets: Vec<UtcOffsetSpan>,
    /// Session open, minutes after local midnight.
    pub session_open_minute: u16,
    /// Session close, minutes after local midnight (after the open).
    pub session_close_minute: u16,
    /// Bit 0 = Monday … bit 6 = Sunday.
    pub trading_weekdays: u8,
    /// Local civil days without a session, as days since 1970-01-01.
    pub holidays: Vec<i32>,
    pub early_closes: Vec<EarlyClose>,
}

/// The calendar bars align to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum TradingCalendar {
    /// Continuous UTC trading (crypto): days at UTC midnight, ISO weeks, months.
    Utc24x7,
    Exchange {
        calendar: ExchangeCalendar,
    },
}

/// Which bar versions a resolved view keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FinalityFilter {
    FinalOnly,
    IncludeProvisional,
}

/// One time-series store point exactly as `TsAppend`/`TsRange` carry it: the
/// bar's `open_time` and its fields in `eg_compute::finance::market::codec`'s
/// fixed layout.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SeriesPoint {
    pub ts: i64,
    pub values: Vec<f64>,
}
