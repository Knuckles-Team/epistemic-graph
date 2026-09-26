//! Durable EH-557 input and budget state for a repository enrichment consumer.
//!
//! The source envelope may only carry a bounded notification. This complete,
//! path-free eligible set must be stored under the same source revision before
//! a consumer can resume pagination after a crash. These types do not reserve
//! budget or submit work: the server must commit checkpoint changes together
//! with native WorkItem admission.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MAX_ELIGIBLE_UNITS: usize = 4_096;
// Matches the server's repository CAS per-body admission bound.
pub const MAX_REPOSITORY_CONTENT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_ID_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnrichmentWorkStage {
    Classical,
    Embedding,
    Llm,
}

/// An engine-attested candidate, ordered deterministically by the producer.
/// The CAS reference points to bytes held by this tenant/repository; the
/// consumer must verify that holder again before submitting a WorkItem.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EligibleUnit {
    pub content_digest: String,
    pub parser_capability_digest: String,
    pub stage: EnrichmentWorkStage,
    pub input_ref: String,
    pub content_length: u64,
    pub compute_units: u64,
    pub demanded: bool,
}

/// Full immutable input to the page planner. It contains no source bytes or
/// logical paths. A content-addressed copy can exceed an outbox event's byte
/// cap, so the event should carry only its digest and durable reference.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EligibleSnapshot {
    pub schema_version: u8,
    pub tenant_id: String,
    pub graph: String,
    pub repository_id: String,
    pub source_envelope: String,
    pub source_commit_ref: String,
    pub policy_digest: String,
    pub catalog_digest: String,
    pub model_digest: String,
    pub budget_units: u64,
    pub units: Vec<EligibleUnit>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotError {
    InvalidIdentity,
    InvalidDigest,
    InvalidCandidate,
    DuplicateCandidate,
    NonDeterministicOrder,
    TooManyCandidates,
    InvalidBudget,
    InvalidCheckpoint,
    EncodeFailure,
}

fn bounded_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_ID_BYTES && !value.chars().any(char::is_control)
}

fn sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

impl EligibleSnapshot {
    /// Validate every durable field before accepting a producer snapshot.
    pub fn validate(&self) -> Result<(), SnapshotError> {
        if self.schema_version != 1
            || [
                &self.tenant_id,
                &self.graph,
                &self.repository_id,
                &self.source_envelope,
                &self.source_commit_ref,
            ]
            .into_iter()
            .any(|value| !bounded_id(value))
        {
            return Err(SnapshotError::InvalidIdentity);
        }
        if [
            &self.policy_digest,
            &self.catalog_digest,
            &self.model_digest,
        ]
        .into_iter()
        .any(|value| !sha256_hex(value))
        {
            return Err(SnapshotError::InvalidDigest);
        }
        if self.budget_units == 0 {
            return Err(SnapshotError::InvalidBudget);
        }
        if self.units.len() > MAX_ELIGIBLE_UNITS {
            return Err(SnapshotError::TooManyCandidates);
        }
        let mut seen = BTreeSet::new();
        let mut previous = None;
        for unit in &self.units {
            let Some(content_digest) = unit.content_digest.strip_prefix("sha256:") else {
                return Err(SnapshotError::InvalidCandidate);
            };
            let Some(input_digest) = unit.input_ref.strip_prefix("cas:sha256:") else {
                return Err(SnapshotError::InvalidCandidate);
            };
            if !sha256_hex(content_digest)
                || !sha256_hex(input_digest)
                || !bounded_id(&unit.parser_capability_digest)
                || unit.content_length > MAX_REPOSITORY_CONTENT_BYTES
                || unit.compute_units == 0
            {
                return Err(SnapshotError::InvalidCandidate);
            }
            let key = (&unit.content_digest, &unit.parser_capability_digest);
            if !seen.insert(key) {
                return Err(SnapshotError::DuplicateCandidate);
            }
            let sort_key = (!unit.demanded, key);
            if previous.is_some_and(|prior| prior > sort_key) {
                return Err(SnapshotError::NonDeterministicOrder);
            }
            previous = Some(sort_key);
        }
        Ok(())
    }

    /// Digest of the validated, deterministically ordered wire record.
    pub fn digest(&self) -> Result<String, SnapshotError> {
        self.validate()?;
        let bytes = rmp_serde::to_vec_named(self).map_err(|_| SnapshotError::EncodeFailure)?;
        let mut hash = Sha256::new();
        hash.update(b"eg/repository-enrichment-snapshot/v1");
        hash.update(bytes);
        Ok(format!("{:x}", hash.finalize()))
    }
}

/// Durable consumer checkpoint. `reserved_units` is admitted work whose final
/// cost has not been settled. `spent_units` is final usage. The three counters
/// must partition the immutable budget exactly. The server must advance this
/// record atomically with WorkItem admission, using `last_batch_key` for replay.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EnrichmentBudgetCheckpoint {
    pub schema_version: u8,
    pub snapshot_digest: String,
    pub next_index: usize,
    pub page_number: u32,
    pub reserved_units: u64,
    pub spent_units: u64,
    pub remaining_units: u64,
    pub last_batch_key: Option<String>,
}

impl EnrichmentBudgetCheckpoint {
    pub fn initial(snapshot: &EligibleSnapshot) -> Result<Self, SnapshotError> {
        Ok(Self {
            schema_version: 1,
            snapshot_digest: snapshot.digest()?,
            next_index: 0,
            page_number: 0,
            reserved_units: 0,
            spent_units: 0,
            remaining_units: snapshot.budget_units,
            last_batch_key: None,
        })
    }

    pub fn validate(&self, snapshot: &EligibleSnapshot) -> Result<(), SnapshotError> {
        if self.schema_version != 1
            || self.snapshot_digest != snapshot.digest()?
            || self.next_index > snapshot.units.len()
            || self.page_number as usize > self.next_index
            || self
                .spent_units
                .checked_add(self.reserved_units)
                .and_then(|used| used.checked_add(self.remaining_units))
                != Some(snapshot.budget_units)
            || self.last_batch_key.as_deref().is_some_and(|key| {
                !key.strip_prefix("repository-enrichment-page:")
                    .is_some_and(sha256_hex)
            })
            || (self.page_number == 0) != self.last_batch_key.is_none()
            || (self.page_number == 0 && self.next_index != 0)
            || (self.page_number == 0 && (self.reserved_units != 0 || self.spent_units != 0))
        {
            return Err(SnapshotError::InvalidCheckpoint);
        }
        Ok(())
    }

    /// Stable replay identity for one immutable snapshot and exact page span.
    pub fn page_key(
        &self,
        snapshot: &EligibleSnapshot,
        end_index: usize,
    ) -> Result<String, SnapshotError> {
        self.validate(snapshot)?;
        if end_index <= self.next_index || end_index > snapshot.units.len() {
            return Err(SnapshotError::InvalidCheckpoint);
        }
        let mut hash = Sha256::new();
        hash.update(b"eg/repository-enrichment-page/v1");
        hash.update(self.snapshot_digest.as_bytes());
        hash.update(self.page_number.to_be_bytes());
        hash.update(self.next_index.to_be_bytes());
        hash.update(end_index.to_be_bytes());
        Ok(format!("repository-enrichment-page:{:x}", hash.finalize()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> EligibleSnapshot {
        EligibleSnapshot {
            schema_version: 1,
            tenant_id: "tenant".into(),
            graph: "code".into(),
            repository_id: "repository".into(),
            source_envelope: "repository-index:one".into(),
            source_commit_ref: "commit:one".into(),
            policy_digest: "a".repeat(64),
            catalog_digest: "b".repeat(64),
            model_digest: "c".repeat(64),
            budget_units: 8,
            units: vec![EligibleUnit {
                content_digest: format!("sha256:{}", "d".repeat(64)),
                parser_capability_digest: "grammar:v1".into(),
                stage: EnrichmentWorkStage::Classical,
                input_ref: format!("cas:sha256:{}", "e".repeat(64)),
                content_length: 42,
                compute_units: 3,
                demanded: false,
            }],
        }
    }

    #[test]
    fn snapshot_round_trip_and_digest_bind_every_authority_field() {
        let first = snapshot();
        let encoded = rmp_serde::to_vec_named(&first).unwrap();
        let decoded: EligibleSnapshot = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(first, decoded);
        assert_eq!(first.digest().unwrap(), decoded.digest().unwrap());
        let mut changed = first.clone();
        changed.model_digest = "f".repeat(64);
        assert_ne!(first.digest().unwrap(), changed.digest().unwrap());
    }

    #[test]
    fn rejects_unbounded_or_untrusted_candidate_shapes() {
        let mut bad = snapshot();
        bad.units = vec![bad.units[0].clone(); MAX_ELIGIBLE_UNITS + 1];
        assert_eq!(bad.validate(), Err(SnapshotError::TooManyCandidates));
        let mut bad = snapshot();
        bad.units[0].input_ref = "path:/tmp/source".into();
        assert_eq!(bad.validate(), Err(SnapshotError::InvalidCandidate));
        let mut bad = snapshot();
        bad.units.push(bad.units[0].clone());
        assert_eq!(bad.validate(), Err(SnapshotError::DuplicateCandidate));
        let mut bad = snapshot();
        bad.units[0].content_length = MAX_REPOSITORY_CONTENT_BYTES + 1;
        assert_eq!(bad.validate(), Err(SnapshotError::InvalidCandidate));
        let mut empty = snapshot();
        empty.units[0].content_length = 0;
        assert!(empty.validate().is_ok());
        let mut bad = snapshot();
        let mut earlier = bad.units[0].clone();
        earlier.content_digest = format!("sha256:{}", "a".repeat(64));
        bad.units.push(earlier);
        assert_eq!(bad.validate(), Err(SnapshotError::NonDeterministicOrder));
    }

    #[test]
    fn checkpoint_rejects_overspend_or_wrong_snapshot_and_replays_same_page_key() {
        let source = snapshot();
        let mut checkpoint = EnrichmentBudgetCheckpoint::initial(&source).unwrap();
        checkpoint.validate(&source).unwrap();
        assert_eq!(
            checkpoint.page_key(&source, 1),
            checkpoint.page_key(&source, 1)
        );
        checkpoint.reserved_units = 9;
        assert_eq!(
            checkpoint.validate(&source),
            Err(SnapshotError::InvalidCheckpoint)
        );
        checkpoint.reserved_units = 0;
        checkpoint.last_batch_key = Some("path:/tmp/spoof".into());
        assert_eq!(
            checkpoint.validate(&source),
            Err(SnapshotError::InvalidCheckpoint)
        );
        let checkpoint = EnrichmentBudgetCheckpoint::initial(&source).unwrap();
        let mut different = source.clone();
        different.source_commit_ref = "commit:two".into();
        assert_eq!(
            checkpoint.validate(&different),
            Err(SnapshotError::InvalidCheckpoint)
        );
    }
}
