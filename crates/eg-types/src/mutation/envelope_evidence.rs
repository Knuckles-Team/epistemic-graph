use super::envelope::MutationEnvelopeV1;
use crate::contract::{Digest256V1, OpaqueIdV1, RequestedMutationResultV1};
use crate::outbox::OutboxIntentV1;

impl MutationEnvelopeV1 {
    pub fn requested_result(&self) -> &RequestedMutationResultV1 {
        &self.requested_result
    }

    pub fn canonical_payload_digest(&self) -> Digest256V1 {
        self.canonical_payload_digest
    }

    pub fn authority_receipt_id(&self) -> &OpaqueIdV1 {
        &self.verified_authority.receipt_id
    }

    pub fn authority_evidence_digest(&self) -> Digest256V1 {
        self.verified_authority.evidence_digest
    }

    pub fn operation_replay_digest(&self) -> Digest256V1 {
        self.operation_replay_digest
    }

    pub fn nonce_replay_digest(&self) -> Digest256V1 {
        self.nonce_replay_digest
    }

    pub fn envelope_digest(&self) -> Digest256V1 {
        self.envelope_digest
    }

    pub fn outbox_intent(&self, index: usize) -> Option<&OutboxIntentV1> {
        self.outbox.as_slice().get(index)
    }
}
