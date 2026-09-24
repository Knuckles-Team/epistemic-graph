//! Server-side chart decimation (EH-420): M4 over integer bars and indicator
//! lines, so a long history reaches a browser as at most a few points per pixel
//! column while every column's open, close, high and low stay exact.

use serde::{Deserialize, Serialize};

use super::bars::BarRecord;
use super::signal::IndicatorPoint;

/// Decimate ordered bars, and indicator series aligned to them, to `width`
/// pixel columns. Indicators are computed over the FULL history first; this
/// only thins what is drawn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecimateRequest {
    pub bars: Vec<BarRecord>,
    /// Each series has one point per bar, at the bar's open time (as
    /// `indicators` returns it).
    #[serde(default)]
    pub indicators: Vec<Vec<IndicatorPoint>>,
    /// Pixel columns the chart spans.
    pub width: u32,
}

/// The drawable chart: at most `width` bar buckets, and per indicator at most
/// four points per column plus every trend-direction change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecimatedChart {
    /// One bar per non-empty column: the first open, highest high, lowest low,
    /// last close and summed volume of the bars in it; final only when every
    /// part is final.
    pub bars: Vec<BarRecord>,
    pub indicators: Vec<Vec<IndicatorPoint>>,
    /// Bars in the request.
    pub source_bars: u64,
    /// False when the request already fit and came back unchanged.
    pub decimated: bool,
}
