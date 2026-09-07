use serde::{Deserialize, Serialize};

use super::envelope::MutationEnvelope;
use super::payload::digest_sequence;
use crate::authority::{AuthorityScope, ReplayReceipt};
use crate::contract::{
    BoundedVec, Digest256, MutationDisposition, OpaqueId, RecordBytes,
    RequestedMutationResult, ResourceId, SchemaId, UtcUnixNanos, MAX_MUTATION_EFFECTS,
};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MutationResult {
    ReceiptOnly,
    ChangedRecords {
        record_digests: BoundedVec<Digest256, MAX_MUTATION_EFFECTS>,
    },
    DomainResult {
        schema_id: SchemaId,
        payload: RecordBytes,
        payload_digest: Digest256,
    },
    NoEffect {
        reason: ResourceId,
    },
}

impl MutationResult {
    pub fn validate(&self) -> Result<(), String> {
        if let Self::DomainResult {
            payload,
            payload_digest,
            ..
        } = self
        {
            if payload.digest()? != *payload_digest {
                return Err("mutation result digest does not match its bytes".into());
            }
        }
        Ok(())
    }

    pub fn digest(&self) -> Result<Digest256, String> {
        self.validate()?;
        match self {
            Self::ReceiptOnly => Digest256::framed(b"eg/mutation-result/v1", &[b"receipt_only"]),
            Self::ChangedRecords { record_digests } => {
                let records = digest_sequence(
                    b"eg/mutation-result-records/v1",
                    record_digests.iter().copied().map(Ok),
                )?;
                Digest256::framed(
                    b"eg/mutation-result/v1",
                    &[b"changed_records", records.as_bytes()],
                )
            }
            Self::DomainResult {
                schema_id,
                payload_digest,
                ..
            } => Digest256::framed(
                b"eg/mutation-result/v1",
                &[
                    b"domain_result",
                    schema_id.as_str().as_bytes(),
                    payload_digest.as_bytes(),
                ],
            ),
            Self::NoEffect { reason } => Digest256::framed(
                b"eg/mutation-result/v1",
                &[b"no_effect", reason.as_str().as_bytes()],
            ),
        }
    }
}

pub(super) fn result_matches_request(
    requested: &RequestedMutationResult,
    result: &MutationResult,
) -> bool {
    matches!(result, MutationResult::ReceiptOnly) && requested.as_str() == "receipt_only"
        || matches!(result, MutationResult::ChangedRecords { .. })
            && requested.as_str() == "changed_records"
        || matches!(result, MutationResult::DomainResult { .. })
            && requested.as_str() == "domain_result"
}

fn receipt_binds_exact_envelope(
    receipt: &MutationReceipt,
    envelope: &MutationEnvelope,
) -> bool {
    receipt.mutation_id == envelope.mutation_id
        && receipt.scope == envelope.scope
        && receipt.authority_receipt_id == envelope.verified_authority.receipt_id
        && receipt.authority_evidence_digest == envelope.verified_authority.evidence_digest
        && receipt.operation_replay_digest == envelope.operation_replay_digest
        && receipt.nonce_replay_digest == envelope.nonce_replay_digest
        && receipt.envelope_digest == envelope.envelope_digest
}

fn receipt_result_matches_request(
    receipt: &MutationReceipt,
    envelope: &MutationEnvelope,
) -> bool {
    !matches!(receipt.disposition.as_str(), "committed" | "replayed")
        || result_matches_request(&envelope.requested_result, &receipt.result)
}

fn original_commit_binds_replay(receipt: &MutationReceipt, replay: &ReplayReceipt) -> bool {
    replay.result_digest == Some(receipt.result_digest)
        && replay.effect_id.as_ref() == receipt.effect_id.as_ref()
        && replay.commit_id.as_ref() == receipt.commit_id.as_ref()
        && replay.effect_digest == receipt.effect_digest
}

fn original_commit_has_no_prior(replay: &ReplayReceipt) -> bool {
    replay.prior_replay_receipt_id.is_none()
        && replay.prior_commit_id.is_none()
        && replay.prior_effect_digest.is_none()
        && replay.prior_result_digest.is_none()
}

fn duplicate_commit_binds_prior(receipt: &MutationReceipt, replay: &ReplayReceipt) -> bool {
    replay.result_digest == Some(receipt.result_digest)
        && replay.prior_effect_id.as_ref() == receipt.effect_id.as_ref()
        && replay.prior_commit_id.as_ref() == receipt.commit_id.as_ref()
        && replay.prior_effect_digest == receipt.effect_digest
        && replay.prior_result_digest == Some(receipt.result_digest)
        && replay.prior_replay_receipt_id.is_some()
}

fn receipt_matches_replay_lifecycle(receipt: &MutationReceipt, replay: &ReplayReceipt) -> bool {
    match (
        receipt.disposition.as_str(),
        replay.status.as_str(),
        replay.effect_state.as_str(),
    ) {
        ("committed", "consumed", "committed") => {
            original_commit_binds_replay(receipt, replay) && original_commit_has_no_prior(replay)
        }
        ("replayed", "duplicate", "committed") => duplicate_commit_binds_prior(receipt, replay),
        ("rejected" | "conflict", "consumed", "failed_terminal") => true,
        ("unavailable", "consumed", "failed_retryable") => true,
        _ => false,
    }
}

fn receipt_effect_binds_envelope(
    receipt: &MutationReceipt,
    envelope: &MutationEnvelope,
) -> Result<bool, String> {
    match receipt.disposition.as_str() {
        "committed" | "replayed" => {
            Ok(receipt.effect_digest == Some(envelope.recompute_effect_digest()?))
        }
        _ => Ok(true),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationReceipt {
    pub receipt_id: OpaqueId,
    pub mutation_id: OpaqueId,
    pub scope: AuthorityScope,
    pub authority_receipt_id: OpaqueId,
    pub authority_evidence_digest: Digest256,
    pub disposition: MutationDisposition,
    pub operation_replay_digest: Digest256,
    pub nonce_replay_digest: Digest256,
    pub envelope_digest: Digest256,
    pub effect_id: Option<OpaqueId>,
    pub effect_digest: Option<Digest256>,
    pub result_digest: Digest256,
    pub commit_id: Option<OpaqueId>,
    pub result: MutationResult,
    pub recorded_at: UtcUnixNanos,
}

impl MutationReceipt {
    pub fn validate(&self) -> Result<(), String> {
        self.result.validate()?;
        if self.result_digest != self.result.digest()? {
            return Err("mutation receipt result digest mismatch".into());
        }
        match self.disposition.as_str() {
            "committed" | "replayed"
                if self.effect_id.is_none()
                    || self.effect_digest.is_none()
                    || self.commit_id.is_none()
                    || matches!(self.result, MutationResult::NoEffect { .. }) =>
            {
                Err("successful mutation requires effect, commit, and success result".into())
            }
            "rejected" | "conflict" | "unavailable"
                if self.effect_id.is_some()
                    || self.effect_digest.is_some()
                    || self.commit_id.is_some()
                    || !matches!(self.result, MutationResult::NoEffect { .. }) =>
            {
                Err("failed mutation cannot expose effect/commit/success result".into())
            }
            _ => Ok(()),
        }
    }

    pub fn validate_against(&self, envelope: &MutationEnvelope) -> Result<(), String> {
        self.validate()?;
        envelope.validate_untrusted_request()?;
        if !receipt_binds_exact_envelope(self, envelope) {
            return Err("mutation receipt differs from its exact envelope".into());
        }
        if self.recorded_at < envelope.verified_authority.replay_receipt.recorded_at {
            return Err("mutation receipt predates replay/admission evidence".into());
        }
        if !receipt_result_matches_request(self, envelope) {
            return Err("mutation result variant differs from requested_result".into());
        }
        let replay = &envelope.verified_authority.replay_receipt;
        if !receipt_matches_replay_lifecycle(self, replay) {
            return Err("mutation disposition/result differs from durable replay lifecycle".into());
        }
        if !receipt_effect_binds_envelope(self, envelope)? {
            return Err("mutation receipt effect digest differs from its envelope".into());
        }
        Ok(())
    }
}
