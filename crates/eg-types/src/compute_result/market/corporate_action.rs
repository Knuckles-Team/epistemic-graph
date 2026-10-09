//! Point-in-time corporate actions (EG-FINANCE-PRIMITIVES-R006).
//!
//! A corporate action is a fact about one immutable listing/instrument
//! identity: a split, a dividend, a symbol change or a delisting. Actions are
//! appended, never rewritten; a correction is a new record for the same
//! `(listing_id, kind, effective_time)` key with a higher `revision`.
//! `known_at` records when that version became knowable, so an earlier view
//! can be reconstructed as of a point in time. A symbol change records only
//! the ticker string change; identity stays keyed by the opaque `listing_id`,
//! so a ticker reused later by an unrelated listing is never conflated with
//! the original economic identity.

use serde::{Deserialize, Serialize};

/// The kind of corporate action and its kind-specific terms.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CorporateActionKind {
    /// A share split or reverse split: `to` new shares for every `from` old.
    Split { from: u32, to: u32 },
    /// A cash dividend, in integer ticks of the listing's currency.
    Dividend { amount_ticks: i64 },
    /// The listing's ticker changed; the economic identity (`listing_id`) does
    /// not. `new_ticker` is the display symbol only, never an identity key.
    SymbolChange { new_ticker: String },
    /// The listing stopped trading as of `effective_time`.
    Delisting,
}

/// One version of one point-in-time corporate action fact.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CorporateAction {
    /// The immutable listing/instrument identity this action affects.
    pub listing_id: String,
    pub action: CorporateActionKind,
    /// UTC nanoseconds the action takes effect.
    pub effective_time: i64,
    /// When this version became knowable (announcement/source time).
    pub known_at: i64,
    /// The source this fact was recorded from.
    pub source: String,
    /// 0 for the first version; each correction is strictly higher.
    pub revision: u32,
}

/// The market session a price/valuation result was observed in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum Session {
    Regular,
    Pre,
    Post,
    Closed,
    /// No calendar was available to classify the timestamp.
    Unknown,
}
