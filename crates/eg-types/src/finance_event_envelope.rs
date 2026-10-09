//! Finance alert event envelope (EG-FINANCE-PRIMITIVES-R011.1).
//!
//! A stable, typed envelope for the finance alerts (price, trend-flip,
//! DCA-due, margin-threshold) that get published through the finance outbox
//! topic family (`outbox::OutboxIntent`). This module carries only the typed
//! model, its validation, and refusal tests; the transactional outbox append
//! path and the outbox-topic wiring are a later slice of EG-FINANCE-PRIMITIVES-R011.
//!
//! `event_id` is immutable once assigned: for `TrendFlip` alerts it is the
//! existing flip event id (see `eg-compute::finance::market::TrendFlip` and
//! `eg-compute::finance::market::events::flip_event`), so replays never
//! re-mint a new identity for the same flip.

#[cfg(feature = "finance")]
use serde::{Deserialize, Serialize};

/// The finance alert conditions this envelope can carry.
#[cfg(feature = "finance")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FinanceAlertKind {
    Price,
    TrendFlip,
    DcaDue,
    MarginThreshold,
}

#[cfg(feature = "finance")]
impl FinanceAlertKind {
    /// The outbox `event_schema` name this alert kind publishes under.
    pub fn outbox_event_schema(&self) -> &'static str {
        match self {
            FinanceAlertKind::Price => "finance.alert.price.v1",
            FinanceAlertKind::TrendFlip => "finance.alert.trend_flip.v1",
            FinanceAlertKind::DcaDue => "finance.alert.dca_due.v1",
            FinanceAlertKind::MarginThreshold => "finance.alert.margin_threshold.v1",
        }
    }
}

/// A typed finance alert envelope staged for the finance outbox topic family.
#[cfg(feature = "finance")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FinanceAlertEnvelope {
    pub event_id: String,
    pub kind: FinanceAlertKind,
    pub listing_id: String,
    pub occurred_at_unix_nanos: i128,
    pub message: String,
}

#[cfg(feature = "finance")]
impl FinanceAlertEnvelope {
    /// Validate the envelope is well-formed before it is handed to the
    /// outbox append path. Refuses an empty identity, an empty listing, a
    /// non-positive timestamp, or an empty message -- none of these can be
    /// round-tripped as a stable alert identity.
    pub fn validate(&self) -> Result<(), String> {
        if self.event_id.trim().is_empty() {
            return Err("finance alert envelope requires a non-empty event_id".into());
        }
        if self.listing_id.trim().is_empty() {
            return Err("finance alert envelope requires a non-empty listing_id".into());
        }
        if self.occurred_at_unix_nanos <= 0 {
            return Err("finance alert envelope requires a positive occurred_at_unix_nanos".into());
        }
        if self.message.trim().is_empty() {
            return Err("finance alert envelope requires a non-empty message".into());
        }
        Ok(())
    }

    /// The outbox event-schema name this alert's kind publishes under.
    pub fn outbox_event_schema(&self) -> &'static str {
        self.kind.outbox_event_schema()
    }
}

#[cfg(all(test, feature = "finance"))]
mod tests {
    use super::*;

    fn valid() -> FinanceAlertEnvelope {
        FinanceAlertEnvelope {
            event_id: "flip-2026-10-09T00:00:00Z-abc123".to_string(),
            kind: FinanceAlertKind::TrendFlip,
            listing_id: "NASDAQ:AAPL".to_string(),
            occurred_at_unix_nanos: 1_760_000_000_000_000_000,
            message: "bullish trend flip".to_string(),
        }
    }

    #[test]
    fn a_well_formed_envelope_validates() {
        assert!(valid().validate().is_ok());
    }

    #[test]
    fn an_empty_event_id_is_refused() {
        let mut envelope = valid();
        envelope.event_id = "  ".to_string();
        assert!(envelope.validate().is_err());
    }

    #[test]
    fn an_empty_listing_id_is_refused() {
        let mut envelope = valid();
        envelope.listing_id = String::new();
        assert!(envelope.validate().is_err());
    }

    #[test]
    fn a_non_positive_timestamp_is_refused() {
        let mut envelope = valid();
        envelope.occurred_at_unix_nanos = 0;
        assert!(envelope.validate().is_err());
    }

    #[test]
    fn an_empty_message_is_refused() {
        let mut envelope = valid();
        envelope.message = String::new();
        assert!(envelope.validate().is_err());
    }

    #[test]
    fn each_alert_kind_has_a_distinct_outbox_schema() {
        let mut schemas = vec![
            FinanceAlertKind::Price.outbox_event_schema(),
            FinanceAlertKind::TrendFlip.outbox_event_schema(),
            FinanceAlertKind::DcaDue.outbox_event_schema(),
            FinanceAlertKind::MarginThreshold.outbox_event_schema(),
        ];
        let before = schemas.len();
        schemas.sort_unstable();
        schemas.dedup();
        assert_eq!(schemas.len(), before);
    }
}
