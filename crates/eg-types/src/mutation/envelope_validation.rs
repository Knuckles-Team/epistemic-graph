use super::envelope::{MutationPayload, MutationRequestEnvelope, MutationRequestEnvelopeParts};
use super::payload::{validate_payload_byte_budget, validate_payload_shape_and_scope};
use super::MUTATION_ENVELOPE_SCHEMA_V1;
use crate::contract::{Digest256, ResourceId, MAX_MUTATION_ENVELOPE_BYTES};
use crate::msgpack::{validate_single_value, MsgpackLimits};

impl MutationRequestEnvelope {
    /// Builds a structurally checked untrusted envelope without a
    /// serialize-then-deserialize round trip. Success does not authorize or
    /// admit execution.
    pub fn new(parts: MutationRequestEnvelopeParts) -> Result<Self, String> {
        parts.payload.validate()?;
        let operation_replay_digest = parts.operation_identity.digest()?;
        let nonce_replay_digest = parts.nonce_replay_key.digest()?;
        let canonical_payload_digest = parts.payload.canonical_payload_digest()?;
        let MutationPayload {
            mutation_id,
            scope,
            preconditions,
            effects,
            outbox,
            provenance,
            requested_result,
        } = parts.payload;
        let mut envelope = Self {
            schema_version: ResourceId::new(MUTATION_ENVELOPE_SCHEMA_V1)?,
            mutation_id,
            verified_authority: parts.verified_authority,
            operation_identity: parts.operation_identity,
            operation_replay_digest,
            nonce_replay_key: parts.nonce_replay_key,
            nonce_replay_digest,
            scope,
            preconditions,
            effects,
            outbox,
            provenance,
            requested_result,
            canonical_payload_digest,
            envelope_digest: Digest256::from_bytes([0; 32]),
        };
        envelope.envelope_digest = envelope.recompute_envelope_digest()?;
        envelope.validate_untrusted_request()?;
        Ok(envelope)
    }

    /// Checks only untrusted DTO shape and immutable evidence commitments. It
    /// does not verify cryptography, consult trusted registries/policy/time,
    /// consume replay state, create an admitted plan, or authorize execution.
    pub fn validate_untrusted_request(&self) -> Result<(), String> {
        self.validate_identity_shape()?;
        if self.scope != self.operation_identity.authority_scope
            || self.scope != self.verified_authority.context.authority_scope
        {
            return Err("mutation scope differs from verified operation authority".into());
        }
        let tenant = &self.verified_authority.context.tenant;
        validate_payload_shape_and_scope(
            tenant,
            &self.scope,
            &self.preconditions,
            &self.effects,
            &self.outbox,
            &self.provenance,
        )?;
        validate_payload_byte_budget(
            &self.preconditions,
            &self.effects,
            &self.outbox,
            &self.provenance,
        )?;
        self.verified_authority
            .validate_evidence_bindings(&self.operation_identity, &self.nonce_replay_key)?;
        if self.recompute_egress_authorization_digest()?
            != self.verified_authority.decision_egress_authorization_digest
        {
            return Err("mutation egress destinations differ from authority decision".into());
        }
        self.validate_digests()
    }

    fn validate_identity_shape(&self) -> Result<(), String> {
        if self.schema_version.as_str() != MUTATION_ENVELOPE_SCHEMA_V1 {
            return Err("mutation schema must be mutation-envelope.v1".into());
        }
        if self.verified_authority.context.operation.as_str() != "mutation" {
            return Err("mutation envelope requires mutation authority".into());
        }
        if self.operation_identity.method.as_str() != "mutation.apply"
            || self.operation_identity.method_schema_id.as_str() != MUTATION_ENVELOPE_SCHEMA_V1
        {
            return Err("mutation envelope method/schema is not catalog-bound".into());
        }
        Ok(())
    }

    fn validate_digests(&self) -> Result<(), String> {
        if self.operation_replay_digest != self.operation_identity.digest()?
            || self.nonce_replay_digest != self.nonce_replay_key.digest()?
            || self.canonical_payload_digest != self.recompute_canonical_payload_digest()?
            || self.operation_identity.canonical_payload_digest != self.canonical_payload_digest
            || self.envelope_digest != self.recompute_envelope_digest()?
        {
            return Err("mutation digest binding mismatch".into());
        }
        Ok(())
    }

    /// Decode only after an allocation-free grammar scan has bounded bytes,
    /// collection items, and nesting. Per-field validation then enforces the
    /// smaller semantic collection and record limits.
    pub fn decode_msgpack(input: &[u8]) -> Result<Self, String> {
        validate_single_value(
            input,
            MsgpackLimits::new(MAX_MUTATION_ENVELOPE_BYTES, MAX_MUTATION_ENVELOPE_BYTES, 64),
        )
        .map_err(|_| "mutation envelope failed bounded MessagePack preflight".to_string())?;
        let envelope: Self = rmp_serde::from_slice(input)
            .map_err(|_| "mutation envelope MessagePack decoding failed".to_string())?;
        Ok(envelope)
    }
}
