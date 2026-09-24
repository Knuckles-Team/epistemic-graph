//! The analysis-snapshot record (EH-421): an immutable, content-addressed
//! account of one market analysis as it was shared. Informational only; it
//! never carries positions and never authorises an order.

use serde::{Deserialize, Serialize};

use super::evidence::FlipConfidence;
use super::signal::{DataStatus, Direction, IndicatorSpec, SignalKey, TrendFlip};

/// Who made a claim. Either way it is a claim, never a fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ClaimAuthor {
    Agent,
    Person,
}

/// A source a claim cites: a URL, an engine record, or both.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClaimSource {
    pub title: String,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub record_ref: Option<String>,
}

/// An explanation statement. A snapshot refuses a claim without a source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SnapshotClaim {
    pub text: String,
    pub author: ClaimAuthor,
    pub sources: Vec<ClaimSource>,
}

/// The bars an analysis covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct BarWindow {
    /// Open time of the first bar.
    pub from_open: i64,
    /// Close time of the last bar.
    pub to_close: i64,
    pub bars: u64,
}

/// What the caller asks to seal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AnalysisSnapshotDraft {
    /// The signal key; it must be the key of `spec` over its series.
    pub key: SignalKey,
    pub spec: IndicatorSpec,
    pub window: BarWindow,
    /// The signal state's `source_revision`: the bar versions it read.
    pub source_revision: String,
    /// The point in time the analysis was replayed as of, if any.
    #[serde(default)]
    pub as_of: Option<i64>,
    pub direction: Option<Direction>,
    pub data_status: DataStatus,
    /// Close of the last final bar, ticks.
    #[serde(default)]
    pub last_close: Option<i64>,
    /// The trailing line, milli-ticks.
    #[serde(default)]
    pub line: Option<i64>,
    /// The flips standing in the window, oldest first: the mechanical trigger.
    pub flips: Vec<TrendFlip>,
    #[serde(default)]
    pub confidence: Option<FlipConfidence>,
    /// Decision-log record ids the analysis relied on.
    #[serde(default)]
    pub decision_refs: Vec<String>,
    /// The chart layers shown, so a shared view draws the same picture.
    #[serde(default)]
    pub layers: Vec<String>,
    #[serde(default)]
    pub claims: Vec<SnapshotClaim>,
    /// When the analysis was made (ns since the epoch, UTC).
    pub created_at: i64,
}

/// The notices every snapshot carries, stamped by the engine, never the caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SnapshotNotices {
    pub version: u32,
    pub informational_only: String,
    pub hallucination: String,
    pub mechanical_trigger: String,
}

/// The immutable, content-addressed analysis snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AnalysisSnapshot {
    /// `sha256:<hex>` over the draft and the notices.
    pub digest: String,
    pub draft: AnalysisSnapshotDraft,
    pub notices: SnapshotNotices,
    /// Always true.
    pub informational_only: bool,
    /// Always true: a snapshot never includes positions or account data.
    pub excludes_positions: bool,
}
