//! EG-UNIFIED-DATA-PLANE-R009 — driving change-capture consumers idempotently. This is the `.1`
//! typed-model slice: a named consumer's durable checkpoint position, and the idempotent-apply
//! guard that makes replaying the same or an older position a no-op. Wiring real
//! graph/vector/full-text/CEP consumers through it is a later child.

use std::fmt;

/// A named consumer's last-applied source position, durable per subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerCheckpoint {
    pub consumer_id: String,
    pub position: u64,
}

/// A checkpoint names no consumer: it cannot be durably tracked or looked back up.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmptyConsumerId;

impl fmt::Display for EmptyConsumerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "consumer checkpoint names no consumer_id")
    }
}

impl std::error::Error for EmptyConsumerId {}

impl ConsumerCheckpoint {
    /// Refuses an empty or whitespace-only `consumer_id`.
    pub fn validate(&self) -> Result<(), EmptyConsumerId> {
        if self.consumer_id.trim().is_empty() {
            return Err(EmptyConsumerId);
        }
        Ok(())
    }

    /// Applies `incoming_position` only if it is strictly newer than the current position,
    /// returning whether it was applied. A replayed or stale position is a no-op (`false`),
    /// not an error: this is the idempotency guarantee itself, not a refusal.
    pub fn apply_if_newer(&mut self, incoming_position: u64) -> bool {
        if incoming_position > self.position {
            self.position = incoming_position;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checkpoint(position: u64) -> ConsumerCheckpoint {
        ConsumerCheckpoint {
            consumer_id: "graph-upsert".to_string(),
            position,
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R009.1
    #[test]
    fn a_named_checkpoint_validates() {
        checkpoint(0).validate().unwrap();
    }

    // spec: EG-UNIFIED-DATA-PLANE-R009.1
    #[test]
    fn an_empty_consumer_id_is_refused() {
        let mut bad = checkpoint(0);
        bad.consumer_id = "   ".to_string();
        assert_eq!(bad.validate().unwrap_err(), EmptyConsumerId);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R009.1
    #[test]
    fn a_strictly_newer_position_applies() {
        let mut cp = checkpoint(10);
        assert!(cp.apply_if_newer(11));
        assert_eq!(cp.position, 11);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R009.1
    #[test]
    fn replaying_the_same_position_is_an_idempotent_no_op() {
        let mut cp = checkpoint(10);
        assert!(cp.apply_if_newer(11));
        assert!(!cp.apply_if_newer(11));
        assert_eq!(cp.position, 11);
    }
}
