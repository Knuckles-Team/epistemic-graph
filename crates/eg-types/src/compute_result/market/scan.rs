//! The latest-state scanner over many signals (EH-415).

use serde::{Deserialize, Serialize};

use super::bars::Timeframe;
use super::signal::{DataStatus, Direction, SignalState};

/// Which states a scan keeps. Empty `statuses` keeps every status.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ScanFilter {
    #[serde(default)]
    pub direction: Option<Direction>,
    #[serde(default)]
    pub statuses: Vec<DataStatus>,
    /// Keep only states whose last flip is at or after this time.
    #[serde(default)]
    pub flipped_since: Option<i64>,
    #[serde(default)]
    pub timeframe: Option<Timeframe>,
}

/// A latest-state scan over many signals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ScanRequest {
    pub states: Vec<SignalState>,
    #[serde(default)]
    pub filter: ScanFilter,
    pub limit: u32,
}

/// One scanner row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ScanRow {
    pub key_digest: String,
    pub listing_id: String,
    pub timeframe: Timeframe,
    pub direction: Option<Direction>,
    pub data_status: DataStatus,
    pub last_flip_at: Option<i64>,
    pub flip_reference_price: Option<i64>,
    pub last_close: Option<i64>,
    /// Change since the flip in basis points of the flip price.
    pub change_since_flip_bps: Option<i64>,
}

/// Counts over every state the filter kept (before the row limit), with the
/// denominator stated: one state per signal key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ScanCounts {
    pub total: u64,
    pub bullish: u64,
    pub bearish: u64,
    pub warming: u64,
    pub stale: u64,
    pub unavailable: u64,
}

/// Rows ordered by most recent flip first, then key digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ScanPage {
    pub rows: Vec<ScanRow>,
    pub counts: ScanCounts,
    /// States superseded by a newer state of the same key.
    pub superseded: u64,
}
