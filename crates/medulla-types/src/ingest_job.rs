//! Typed ingest job envelope, lanes, partitioning and retry policy
//! (MEDULLA-INGEST-WORKER-R001.1, R001.2, R001.3, R001.7, R001.8).
//!
//! Pure data and pure functions. The served queue, the claim/ack path and the
//! dead-letter routing are later rows (R001.4, R001.9 to R001.11).

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// The kinds of work the ingest worker drains. The set is closed: an unknown
/// kind fails deserialization.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum IngestJobKind {
    Document,
    Codebase,
    SkillWorkflows,
    CapabilityHydration,
}

/// The queue lane a job kind runs on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum IngestLane {
    General,
    Hydration,
}

impl IngestLane {
    /// The served task-queue name for this lane.
    pub fn queue_name(self) -> &'static str {
        match self {
            Self::General => "ingest",
            Self::Hydration => "ingest-hydration",
        }
    }
}

/// Hydration kinds run on the reserved hydration lane; everything else is general.
pub fn lane_for(kind: IngestJobKind) -> IngestLane {
    match kind {
        IngestJobKind::SkillWorkflows | IngestJobKind::CapabilityHydration => IngestLane::Hydration,
        IngestJobKind::Document | IngestJobKind::Codebase => IngestLane::General,
    }
}

/// One unit of ingest work as the queue carries it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct IngestJobEnvelope {
    pub job_id: String,
    pub tenant: String,
    pub corpus_key: String,
    pub kind: IngestJobKind,
    pub target: String,
    pub attempt: u32,
}

/// Why an ingest job envelope or retry policy is refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum IngestJobError {
    #[error("ingest job_id must not be blank")]
    BlankJobId,
    #[error("ingest target must not be blank")]
    BlankTarget,
    #[error("unknown ingest job kind")]
    UnknownKind,
    #[error("ingest attempt {attempt} exceeds the retry ceiling {max_attempts}")]
    AttemptAboveCeiling { attempt: u32, max_attempts: u32 },
    #[error("ingest retry policy must allow at least one attempt")]
    ZeroAttempts,
    #[error("ingest retry base delay exceeds the max delay")]
    BaseAboveMax,
    #[error("ingest retry jitter must be a finite fraction in [0, 1]")]
    InvalidJitter,
}

impl IngestJobEnvelope {
    /// Parse and validate an envelope from JSON, mapping an unknown kind to a
    /// typed refusal instead of an opaque decode error.
    pub fn from_json(
        value: serde_json::Value,
        policy: &IngestRetryPolicy,
    ) -> Result<Self, IngestJobError> {
        if let Some(kind) = value.get("kind") {
            if serde_json::from_value::<IngestJobKind>(kind.clone()).is_err() {
                return Err(IngestJobError::UnknownKind);
            }
        }
        let envelope: Self =
            serde_json::from_value(value).map_err(|_| IngestJobError::UnknownKind)?;
        envelope.validate(policy)?;
        Ok(envelope)
    }

    /// Refuse a blank id or target and an attempt above the retry ceiling.
    pub fn validate(&self, policy: &IngestRetryPolicy) -> Result<(), IngestJobError> {
        if self.job_id.trim().is_empty() {
            return Err(IngestJobError::BlankJobId);
        }
        if self.target.trim().is_empty() {
            return Err(IngestJobError::BlankTarget);
        }
        if self.attempt > policy.max_attempts {
            return Err(IngestJobError::AttemptAboveCeiling {
                attempt: self.attempt,
                max_attempts: policy.max_attempts,
            });
        }
        Ok(())
    }

    pub fn lane(&self) -> IngestLane {
        lane_for(self.kind)
    }
}

/// The queue partition: tenant, then corpus, then kind. Jobs that share a key
/// keep their order; different keys never serialize against each other.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IngestPartitionKey {
    pub tenant: String,
    pub corpus_key: String,
    pub kind: &'static str,
}

pub fn partition_key(envelope: &IngestJobEnvelope) -> IngestPartitionKey {
    let kind = match envelope.kind {
        IngestJobKind::Document => "document",
        IngestJobKind::Codebase => "codebase",
        IngestJobKind::SkillWorkflows => "skill_workflows",
        IngestJobKind::CapabilityHydration => "capability_hydration",
    };
    IngestPartitionKey {
        tenant: envelope.tenant.clone(),
        corpus_key: envelope.corpus_key.clone(),
        kind,
    }
}

/// Bounded exponential retry with jitter.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct IngestRetryPolicy {
    pub max_attempts: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    /// Fraction of the exponential delay that may be removed at random.
    pub jitter: f64,
}

impl IngestRetryPolicy {
    pub fn validate(&self) -> Result<(), IngestJobError> {
        if self.max_attempts == 0 {
            return Err(IngestJobError::ZeroAttempts);
        }
        if self.base_delay_ms > self.max_delay_ms {
            return Err(IngestJobError::BaseAboveMax);
        }
        if !self.jitter.is_finite() || !(0.0..=1.0).contains(&self.jitter) {
            return Err(IngestJobError::InvalidJitter);
        }
        Ok(())
    }
}

fn mix(seed: u64, attempt: u32) -> u64 {
    // splitmix64 over the seed and attempt: deterministic and dependency-free.
    let mut z = seed ^ (u64::from(attempt)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Delay before redelivering `attempt` (1-based). The un-jittered ceiling is
/// `min(max_delay, base_delay * 2^(attempt-1))`; jitter removes a seeded share
/// of at most `jitter` of it so workers do not retry in lockstep.
pub fn next_delay(policy: &IngestRetryPolicy, attempt: u32, seed: u64) -> Duration {
    let shift = attempt.saturating_sub(1).min(63);
    let ceiling = policy
        .base_delay_ms
        .saturating_mul(1u64 << shift)
        .min(policy.max_delay_ms);
    let unit = (mix(seed, attempt) >> 11) as f64 / (1u64 << 53) as f64;
    let removed = (ceiling as f64 * policy.jitter.clamp(0.0, 1.0) * unit) as u64;
    Duration::from_millis(ceiling - removed.min(ceiling))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn policy() -> IngestRetryPolicy {
        IngestRetryPolicy {
            max_attempts: 5,
            base_delay_ms: 100,
            max_delay_ms: 2_000,
            jitter: 0.5,
        }
    }

    fn envelope() -> IngestJobEnvelope {
        IngestJobEnvelope {
            job_id: "job-1".into(),
            tenant: "t1".into(),
            corpus_key: "repo-a".into(),
            kind: IngestJobKind::Document,
            target: "docs/readme.md".into(),
            attempt: 1,
        }
    }

    // spec: MEDULLA-INGEST-WORKER-R001.1
    #[test]
    fn envelope_round_trips_and_refuses_invalid_shapes() {
        let ok = envelope();
        assert_eq!(ok.validate(&policy()), Ok(()));
        let encoded = serde_json::to_value(&ok).unwrap();
        assert_eq!(encoded["kind"], "document");
        assert_eq!(
            IngestJobEnvelope::from_json(encoded.clone(), &policy()),
            Ok(ok.clone())
        );

        let blank_id = IngestJobEnvelope {
            job_id: " ".into(),
            ..ok.clone()
        };
        assert_eq!(
            blank_id.validate(&policy()),
            Err(IngestJobError::BlankJobId)
        );
        let blank_target = IngestJobEnvelope {
            target: String::new(),
            ..ok.clone()
        };
        assert_eq!(
            blank_target.validate(&policy()),
            Err(IngestJobError::BlankTarget)
        );
        let over = IngestJobEnvelope {
            attempt: 6,
            ..ok.clone()
        };
        assert_eq!(
            over.validate(&policy()),
            Err(IngestJobError::AttemptAboveCeiling {
                attempt: 6,
                max_attempts: 5
            })
        );
        let mut unknown = encoded;
        unknown["kind"] = json!("analytics");
        assert_eq!(
            IngestJobEnvelope::from_json(unknown, &policy()),
            Err(IngestJobError::UnknownKind)
        );
    }

    // spec: MEDULLA-INGEST-WORKER-R001.2
    #[test]
    fn lane_for_every_kind() {
        assert_eq!(lane_for(IngestJobKind::Document), IngestLane::General);
        assert_eq!(lane_for(IngestJobKind::Codebase), IngestLane::General);
        assert_eq!(
            lane_for(IngestJobKind::SkillWorkflows),
            IngestLane::Hydration
        );
        assert_eq!(
            lane_for(IngestJobKind::CapabilityHydration),
            IngestLane::Hydration
        );
        assert_eq!(IngestLane::General.queue_name(), "ingest");
        assert_eq!(IngestLane::Hydration.queue_name(), "ingest-hydration");
    }

    // spec: MEDULLA-INGEST-WORKER-R001.3
    #[test]
    fn partition_key_orders_per_tenant_and_corpus() {
        let a = envelope();
        let b = IngestJobEnvelope {
            job_id: "job-2".into(),
            target: "docs/other.md".into(),
            ..a.clone()
        };
        assert_eq!(partition_key(&a), partition_key(&b));
        let other_tenant = IngestJobEnvelope {
            tenant: "t2".into(),
            ..a.clone()
        };
        assert_ne!(partition_key(&a), partition_key(&other_tenant));
        let other_corpus = IngestJobEnvelope {
            corpus_key: "repo-b".into(),
            ..a.clone()
        };
        assert_ne!(partition_key(&a), partition_key(&other_corpus));
        let key = partition_key(&a);
        assert_eq!(
            (key.tenant.as_str(), key.corpus_key.as_str(), key.kind),
            ("t1", "repo-a", "document")
        );
    }

    // spec: MEDULLA-INGEST-WORKER-R001.7
    #[test]
    fn retry_policy_refusals() {
        assert_eq!(policy().validate(), Ok(()));
        let zero = IngestRetryPolicy {
            max_attempts: 0,
            ..policy()
        };
        assert_eq!(zero.validate(), Err(IngestJobError::ZeroAttempts));
        let inverted = IngestRetryPolicy {
            base_delay_ms: 3_000,
            ..policy()
        };
        assert_eq!(inverted.validate(), Err(IngestJobError::BaseAboveMax));
        let negative = IngestRetryPolicy {
            jitter: -0.1,
            ..policy()
        };
        assert_eq!(negative.validate(), Err(IngestJobError::InvalidJitter));
        let nan = IngestRetryPolicy {
            jitter: f64::NAN,
            ..policy()
        };
        assert_eq!(nan.validate(), Err(IngestJobError::InvalidJitter));
    }

    // spec: MEDULLA-INGEST-WORKER-R001.8
    #[test]
    fn next_delay_is_bounded_growing_and_seeded() {
        let p = policy();
        for attempt in 1..=10 {
            for seed in 0..32 {
                let delay = next_delay(&p, attempt, seed).as_millis() as u64;
                let ceiling = (100u64 << (attempt - 1)).min(2_000);
                assert!(
                    delay <= ceiling && delay >= ceiling / 2,
                    "{attempt} {delay}"
                );
            }
        }
        let flat = IngestRetryPolicy {
            jitter: 0.0,
            ..p.clone()
        };
        let delays: Vec<u64> = (1..=8)
            .map(|a| next_delay(&flat, a, 7).as_millis() as u64)
            .collect();
        assert!(delays.windows(2).all(|w| w[0] <= w[1]));
        assert_eq!(delays[0], 100);
        assert_eq!(*delays.last().unwrap(), 2_000);
        assert_ne!(next_delay(&p, 3, 1), next_delay(&p, 3, 2));
        assert_eq!(next_delay(&p, 3, 1), next_delay(&p, 3, 1));
        assert!(next_delay(&p, u32::MAX, 9) <= Duration::from_millis(2_000));
    }
}
