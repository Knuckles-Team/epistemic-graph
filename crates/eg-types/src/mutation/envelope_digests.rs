use super::effects::MutationEffectV1;
use super::envelope::MutationEnvelopeV1;
use super::payload::{
    digest_sequence, recompute_canonical_payload_digest, recompute_egress_authorization_digest,
};
use crate::contract::Digest256V1;

impl MutationEnvelopeV1 {
    pub fn recompute_canonical_payload_digest(&self) -> Result<Digest256V1, String> {
        recompute_canonical_payload_digest(
            &self.mutation_id,
            &self.scope,
            &self.preconditions,
            &self.effects,
            &self.outbox,
            &self.provenance,
            &self.requested_result,
        )
    }

    pub fn recompute_egress_authorization_digest(&self) -> Result<Digest256V1, String> {
        recompute_egress_authorization_digest(&self.outbox)
    }

    pub fn recompute_envelope_digest(&self) -> Result<Digest256V1, String> {
        Digest256V1::framed(
            b"eg/mutation-envelope/v1",
            &[
                self.schema_version.as_str().as_bytes(),
                self.mutation_id.as_str().as_bytes(),
                self.verified_authority.receipt_id.as_str().as_bytes(),
                self.verified_authority.evidence_digest.as_bytes(),
                self.operation_replay_digest.as_bytes(),
                self.nonce_replay_digest.as_bytes(),
                self.canonical_payload_digest.as_bytes(),
            ],
        )
    }

    pub fn recompute_effect_digest(&self) -> Result<Digest256V1, String> {
        digest_sequence(
            b"eg/mutation-effects/v1",
            self.effects.iter().map(MutationEffectV1::digest),
        )
    }
}
