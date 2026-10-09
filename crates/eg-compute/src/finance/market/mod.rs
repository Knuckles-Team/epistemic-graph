//! Market bars and trend signals (train 6, EH-413..EH-418).
//!
//! * [`codec`], [`resolve`], [`calendar`], [`rollup`] — the typed OHLCV bar-series
//!   contract over the time-series store: point layout, as-of revision
//!   resolution, trading calendars and bar-to-bar rollup (EH-413).
//! * [`kernels`], [`supertrend`], [`indicators`] — incremental integer kernels,
//!   one step per closed bar (EH-414).
//! * [`signal`], [`scan`], [`events`] — per-bar signal advance, bitemporal replay
//!   with flip revisions, the latest-state scanner, and flips as CEP events
//!   (multi-timeframe agreement through eg-stream's NFA) (EH-415).
//! * [`confidence`] — Decide-calibrated flip confidence with abstention (EH-417).
//! * [`backtest_run`] — the backtest-run provenance record (EH-418).
//! * [`decimate`] — M4 chart decimation over integer ticks (EH-420).
//! * [`snapshot`] — the analysis-snapshot record with its notices (EH-421).
//!
//! Prices are integer ticks and indicator values integer milli-ticks, so every
//! output is bit-identical on every target. Nothing here reads a clock or a
//! store, and nothing authorises an order.

pub mod backtest_run;
pub mod calendar;
pub mod codec;
pub mod confidence;
pub mod decimate;
pub mod digest;
pub mod events;
pub mod fixed;
pub mod indicators;
pub mod kernels;
pub mod recommendation;
pub mod resolve;
pub mod rollup;
pub mod scan;
pub mod signal;
pub mod snapshot;
pub mod supertrend;

#[cfg(test)]
mod golden_tests;
#[cfg(test)]
mod record_tests;

pub use eg_types::compute_result::market::*;

/// A typed refusal: a closed code and a detail, rendered `"CODE: detail"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketError {
    pub code: &'static str,
    pub detail: String,
}

impl MarketError {
    pub fn new(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for MarketError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.detail)
    }
}

impl std::error::Error for MarketError {}

pub type MarketResult<T> = Result<T, MarketError>;

/// A bar violates the bar contract.
pub const INVALID_BAR: &str = "INVALID_BAR";
/// Two versions of one bar with the same revision disagree, or a revision
/// would replace a final bar with a provisional one.
pub const CONFLICTING_REVISION: &str = "CONFLICTING_REVISION";
/// Bars overlap or arrive out of order where order is required.
pub const OUT_OF_ORDER: &str = "OUT_OF_ORDER";
/// A bar or calendar the calendar cannot place.
pub const CALENDAR: &str = "CALENDAR";
/// A value outside the exact integer range.
pub const OVERFLOW: &str = "OVERFLOW";
/// An indicator specification or request the operation cannot run.
pub const INVALID_REQUEST: &str = "INVALID_REQUEST";
/// A bar at or before a state's last bar: a revision needs a replay.
pub const REVISION_NEEDS_REPLAY: &str = "REVISION_NEEDS_REPLAY";
/// A fill before its signal was knowable, or outside the point-in-time universe.
pub const LOOK_AHEAD: &str = "LOOK_AHEAD";
/// An analysis claim that cites no source.
pub const UNSOURCED_CLAIM: &str = "UNSOURCED_CLAIM";
