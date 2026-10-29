//! Typed retention-cleanup policy (EG-DURABLE-KERNEL-R700.1).
//!
//! `RetentionPolicy { age, batch, holds }`. The shapes EG-DURABLE-KERNEL-R700.4
//! refuses cannot be built: `holds` is a required field, a policy without any
//! hold check is refused at construction and on decode, and zero age or batch is
//! refused. Scanning and removal are EG-DURABLE-KERNEL-R700.2 onwards.

use serde::{Deserialize, Serialize};

/// The largest batch one cleanup step may remove in a single `MutationBatch`.
pub const MAX_RETENTION_BATCH: u32 = 10_000;

/// A check that keeps a candidate from removal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HoldCheck {
    /// A live inbound reference keeps the node.
    LiveReference,
    /// A legal hold keeps the node.
    LegalHold,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RetentionPolicy {
    age_ms: u64,
    batch: u32,
    holds: Vec<HoldCheck>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RetentionPolicyError {
    #[error("RETENTION_HOLD_CHECK_REQUIRED: a retention policy must declare a hold check")]
    HoldCheckRequired,
    #[error("a retention policy age must be above zero")]
    ZeroAge,
    #[error("a retention batch must be between 1 and {MAX_RETENTION_BATCH}")]
    BatchOutOfRange,
}

impl RetentionPolicy {
    pub fn new(
        age_ms: u64,
        batch: u32,
        mut holds: Vec<HoldCheck>,
    ) -> Result<Self, RetentionPolicyError> {
        holds.sort_unstable();
        holds.dedup();
        if holds.is_empty() {
            return Err(RetentionPolicyError::HoldCheckRequired);
        }
        if age_ms == 0 {
            return Err(RetentionPolicyError::ZeroAge);
        }
        if batch == 0 || batch > MAX_RETENTION_BATCH {
            return Err(RetentionPolicyError::BatchOutOfRange);
        }
        Ok(Self {
            age_ms,
            batch,
            holds,
        })
    }

    pub fn age_ms(&self) -> u64 {
        self.age_ms
    }

    pub fn batch(&self) -> u32 {
        self.batch
    }

    pub fn holds(&self) -> &[HoldCheck] {
        &self.holds
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetentionPolicyWire {
    age_ms: u64,
    batch: u32,
    holds: Vec<HoldCheck>,
}

impl<'de> Deserialize<'de> for RetentionPolicy {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = RetentionPolicyWire::deserialize(deserializer)?;
        Self::new(wire.age_ms, wire.batch, wire.holds).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOLDEN: &str =
        r#"{"age_ms":2592000000,"batch":500,"holds":["live_reference","legal_hold"]}"#;

    // spec: EG-DURABLE-KERNEL-R700.1
    #[test]
    fn golden_vector_round_trips() {
        let policy = RetentionPolicy::new(
            2_592_000_000,
            500,
            vec![
                HoldCheck::LegalHold,
                HoldCheck::LiveReference,
                HoldCheck::LegalHold,
            ],
        )
        .unwrap();
        assert_eq!(serde_json::to_string(&policy).unwrap(), GOLDEN);
        assert_eq!(
            serde_json::from_str::<RetentionPolicy>(GOLDEN).unwrap(),
            policy
        );
        let packed = rmp_serde::to_vec_named(&policy).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<RetentionPolicy>(&packed).unwrap(),
            policy
        );
    }

    // spec: EG-DURABLE-KERNEL-R700.1
    #[test]
    fn invalid_shapes_cannot_be_built_or_decoded() {
        assert_eq!(
            RetentionPolicy::new(1, 1, vec![]),
            Err(RetentionPolicyError::HoldCheckRequired)
        );
        assert_eq!(
            RetentionPolicy::new(0, 1, vec![HoldCheck::LegalHold]),
            Err(RetentionPolicyError::ZeroAge)
        );
        for batch in [0, MAX_RETENTION_BATCH + 1] {
            assert_eq!(
                RetentionPolicy::new(1, batch, vec![HoldCheck::LegalHold]),
                Err(RetentionPolicyError::BatchOutOfRange)
            );
        }
        for bad in [
            r#"{"age_ms":1,"batch":1}"#,
            r#"{"age_ms":1,"batch":1,"holds":[]}"#,
            r#"{"age_ms":1,"batch":1,"holds":["none"]}"#,
            r#"{"age_ms":0,"batch":1,"holds":["legal_hold"]}"#,
            r#"{"age_ms":1,"batch":1,"holds":["legal_hold"],"extra":1}"#,
        ] {
            assert!(
                serde_json::from_str::<RetentionPolicy>(bad).is_err(),
                "{bad}"
            );
        }
        let err = serde_json::from_str::<RetentionPolicy>(r#"{"age_ms":1,"batch":1,"holds":[]}"#)
            .unwrap_err();
        assert!(err.to_string().contains("RETENTION_HOLD_CHECK_REQUIRED"));
    }
}
