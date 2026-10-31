//! Versioned FRED net-liquidity inputs and signed heat-strip components.

use serde::{Deserialize, Serialize};

/// The three series in the Bellwether net-liquidity definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum LiquiditySeries {
    Walcl,
    Wdtgal,
    Rrpontsyd,
}

/// One sourced observation. `value_milliunits` uses thousandths of the
/// series' published unit: millions USD for WALCL/WDTGAL, billions for RRP.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LiquidityObservation {
    pub series: LiquiditySeries,
    pub observation_ms: i64,
    pub vintage_ms: i64,
    /// When this observation was actually captured, not an inferred release time.
    pub observed_ms: i64,
    pub value_milliunits: i64,
    pub source_ref: String,
}

/// An explicitly bounded point-in-time request; timestamps are UTC milliseconds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LiquidityPanelRequest {
    pub version: u32,
    pub as_of_ms: i64,
    pub max_age_ms: i64,
    pub observations: Vec<LiquidityObservation>,
}

/// Direction of a signed liquidity contribution since its previous period.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum LiquidityDirection {
    Increasing,
    Decreasing,
    Unchanged,
    Unavailable,
}

/// One signed contribution and its change from the preceding available period.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LiquidityHeatCell {
    pub series: LiquiditySeries,
    pub contribution_usd: i64,
    pub change_usd: Option<i64>,
    pub direction: LiquidityDirection,
    pub previous_source_ref: Option<String>,
    pub previous_observation_ms: Option<i64>,
    pub previous_vintage_ms: Option<i64>,
    pub previous_observed_ms: Option<i64>,
    pub observation_ms: i64,
    pub vintage_ms: i64,
    pub observed_ms: i64,
    pub source_ref: String,
}

/// Composite and heat strip in exact USD, with source times retained.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LiquidityPanel {
    pub version: u32,
    pub as_of_ms: i64,
    pub max_age_ms: i64,
    pub net_liquidity_usd: i64,
    pub heat: Vec<LiquidityHeatCell>,
}
