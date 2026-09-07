//! Signed, verified, admitted, and decision evidence binding.

use serde::{Deserialize, Serialize};

use super::context::{AuthorityContext, AUTHORITY_PROTOCOL_V1};
use super::replay::{NonceReplayKey, OperationReplayIdentity, ReplayReceipt};
use crate::contract::{
    AdmissionOutcome, DecisionOutcome, Digest256, Ed25519Signature, MethodId, OpaqueId,
    ResourceId, SchemaId, UtcUnixNanos, VerificationStatus,
};

/// Immutable signed-envelope evidence. Validation proves only canonical field
/// binding; signature verification, key-registry status, and trusted time are
/// deliberately request-boundary responsibilities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedAuthorityEnvelopeEvidence {
    pub protocol_id: crate::contract::ProtocolId,
    pub schema_version: ResourceId,
    pub catalog_digest: Digest256,
    pub context_digest: Digest256,
    pub payload_digest: Digest256,
    pub audience: crate::contract::AudienceId,
    pub issued_at: UtcUnixNanos,
    pub expires_at: UtcUnixNanos,
    pub signer_key_id: OpaqueId,
    pub signer_key_version: u64,
    pub signature_algorithm: ResourceId,
    pub canonicalization: ResourceId,
    pub unsigned_message_digest: Digest256,
    pub signature: Ed25519Signature,
    pub envelope_digest: Digest256,
}

impl SignedAuthorityEnvelopeEvidence {
    pub fn validate(&self) -> Result<(), String> {
        if self.protocol_id.as_str() != AUTHORITY_PROTOCOL_V1
            || self.schema_version.as_str() != "signed-authority-envelope.v1"
            || self.signature_algorithm.as_str() != "ed25519"
            || self.canonicalization.as_str() != "canonical-msgpack.v1"
            || self.expires_at <= self.issued_at
        {
            return Err("signed authority envelope evidence shape is invalid".into());
        }
        if self.unsigned_message_digest != self.recompute_unsigned_message_digest()? {
            return Err("signed authority unsigned-message digest mismatch".into());
        }
        if self.envelope_digest != self.recompute_envelope_digest()? {
            return Err("signed authority envelope digest mismatch".into());
        }
        Ok(())
    }

    /// Digest of the complete canonical unsigned message. A trusted verifier
    /// verifies `signature` over this digest after resolving the pinned key.
    pub fn recompute_unsigned_message_digest(&self) -> Result<Digest256, String> {
        Digest256::framed(
            b"eg/signed-authority-unsigned-message/v1",
            &[
                self.protocol_id.as_str().as_bytes(),
                self.schema_version.as_str().as_bytes(),
                self.catalog_digest.as_bytes(),
                self.context_digest.as_bytes(),
                self.payload_digest.as_bytes(),
                self.audience.as_str().as_bytes(),
                &self.issued_at.get().to_be_bytes(),
                &self.expires_at.get().to_be_bytes(),
                self.signer_key_id.as_str().as_bytes(),
                &self.signer_key_version.to_be_bytes(),
                self.signature_algorithm.as_str().as_bytes(),
                self.canonicalization.as_str().as_bytes(),
            ],
        )
    }

    pub fn recompute_envelope_digest(&self) -> Result<Digest256, String> {
        Digest256::framed(
            b"eg/signed-authority-envelope/v1",
            &[
                self.unsigned_message_digest.as_bytes(),
                self.signature.as_bytes(),
            ],
        )
    }
}

/// EG-produced, serializable evidence aggregate. `Deserialize` reconstructs
/// untrusted evidence only; it never reconstructs execution authority.
///
/// The future `eg-transaction` boundary must consume this evidence only after
/// signature/key-registry/current-time/policy/replay verification and mint a
/// crate-private, non-`Deserialize`, non-`Clone` admitted token with no public
/// constructor. `MutationKernel` must require that token, never this DTO.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifiedAuthority {
    pub receipt_id: OpaqueId,
    pub context: AuthorityContext,
    pub signed_envelope: SignedAuthorityEnvelopeEvidence,
    pub verification_receipt_id: OpaqueId,
    pub verification_status: VerificationStatus,
    pub envelope_digest: Digest256,
    pub verified_context_digest: Digest256,
    pub verified_payload_digest: Digest256,
    pub verified_catalog_digest: Digest256,
    pub verified_method: MethodId,
    pub verified_method_schema_id: SchemaId,
    pub verified_method_schema_digest: Digest256,
    pub signer_key_id: OpaqueId,
    pub verified_at: UtcUnixNanos,
    pub verification_valid_until: UtcUnixNanos,
    pub admission_receipt_id: OpaqueId,
    pub admission_decision_id: OpaqueId,
    pub admission_manifest_digest: Digest256,
    pub admission_catalog_digest: Digest256,
    pub admission_context_digest: Digest256,
    pub admission_operation_replay_digest: Digest256,
    pub admission_nonce_replay_digest: Digest256,
    pub admission_policy_digest: Digest256,
    pub admission_egress_authorization_digest: Digest256,
    pub admission_durable_identity_epoch: u64,
    pub admission_policy_epoch: u64,
    pub admission_state: AdmissionOutcome,
    pub admission_issued_at: UtcUnixNanos,
    pub admission_expires_at: UtcUnixNanos,
    pub replay_receipt: ReplayReceipt,
    pub valid_until: UtcUnixNanos,
    pub durable_identity_epoch: u64,
    pub decision_id: OpaqueId,
    pub parent_decision_id: Option<OpaqueId>,
    pub decision_outcome: DecisionOutcome,
    pub decision_context_digest: Digest256,
    pub decision_policy_digest: Digest256,
    pub decision_egress_authorization_digest: Digest256,
    pub decision_policy_epoch: u64,
    pub decision_reason_code: ResourceId,
    pub decision_retryable: bool,
    pub operation_replay_digest: Digest256,
    pub nonce_replay_digest: Digest256,
    pub evidence_digest: Digest256,
}

impl VerifiedAuthority {
    /// Structural evidence validation only. This does not verify a signature,
    /// consult a key registry or policy store, consume replay state, compare a
    /// trusted clock, or admit execution.
    pub(crate) fn validate_evidence_bindings(
        &self,
        operation_identity: &OperationReplayIdentity,
        nonce_key: &NonceReplayKey,
    ) -> Result<(), String> {
        self.context.validate()?;
        self.signed_envelope.validate()?;
        self.replay_receipt.validate()?;
        require(
            verification_time_is_valid(self),
            "verified authority receipt expiry is invalid",
        )?;
        require(
            admission_time_is_valid(self),
            "verified authority receipt expiry is invalid",
        )?;
        require(
            replay_time_is_valid(self),
            "verified authority receipt expiry is invalid",
        )?;
        require(
            authority_state_is_admitted(self),
            "verified authority is not verified, admitted, allowed, and replay-safe",
        )?;
        let operation_digest = operation_identity.digest()?;
        let nonce_digest = nonce_key.digest()?;
        require(
            operation_identity_matches_context(self, operation_identity, nonce_key),
            "verified authority replay identity differs from its context",
        )?;
        let expected_valid_until = std::cmp::min(
            self.context.expires_at,
            std::cmp::min(self.verification_valid_until, self.admission_expires_at),
        );
        require(
            self.valid_until == expected_valid_until,
            "verified authority expiry is not the receipt minimum",
        )?;
        require(
            signed_envelope_matches(self, operation_identity),
            "verified authority replay bindings do not match",
        )?;
        require(
            verification_receipt_matches(self, operation_identity),
            "verified authority replay bindings do not match",
        )?;
        require(
            admission_decision_matches(self),
            "verified authority replay bindings do not match",
        )?;
        require(
            admission_replay_matches(self, operation_digest, nonce_digest),
            "verified authority replay bindings do not match",
        )?;
        require(
            decision_matches(self),
            "verified authority replay bindings do not match",
        )?;
        require(
            replay_receipt_matches(self, operation_digest, nonce_digest),
            "verified authority replay bindings do not match",
        )?;
        require(
            self.evidence_digest == self.recompute_evidence_digest()?,
            "verified authority replay bindings do not match",
        )?;
        Ok(())
    }

    /// Complete immutable evidence commitment. It is not a verification or
    /// admission operation; the trusted boundary must independently verify the
    /// evidence before minting its private executable token.
    pub fn recompute_evidence_digest(&self) -> Result<Digest256, String> {
        self.context.validate()?;
        self.signed_envelope.validate()?;
        let replay = self.replay_receipt.evidence_digest()?;
        let verified_at = self.verified_at.get().to_be_bytes();
        let verification_valid_until = self.verification_valid_until.get().to_be_bytes();
        let admission_identity_epoch = self.admission_durable_identity_epoch.to_be_bytes();
        let admission_policy_epoch = self.admission_policy_epoch.to_be_bytes();
        let admission_issued_at = self.admission_issued_at.get().to_be_bytes();
        let admission_expires_at = self.admission_expires_at.get().to_be_bytes();
        let valid_until = self.valid_until.get().to_be_bytes();
        let identity_epoch = self.durable_identity_epoch.to_be_bytes();
        let decision_policy_epoch = self.decision_policy_epoch.to_be_bytes();
        let decision_retryable = [u8::from(self.decision_retryable)];
        let mut fields: Vec<&[u8]> = Vec::with_capacity(44);
        self.push_verification_fields(&mut fields, &verified_at, &verification_valid_until);
        self.push_admission_fields(
            &mut fields,
            &admission_identity_epoch,
            &admission_policy_epoch,
            &admission_issued_at,
            &admission_expires_at,
        );
        fields.push(replay.as_bytes());
        fields.push(&valid_until);
        fields.push(&identity_epoch);
        self.push_decision_fields(&mut fields, &decision_policy_epoch, &decision_retryable);
        Digest256::framed(b"eg/verified-authority-evidence/v1", &fields)
    }

    fn push_verification_fields<'a>(
        &'a self,
        fields: &mut Vec<&'a [u8]>,
        verified_at: &'a [u8; 8],
        verification_valid_until: &'a [u8; 8],
    ) {
        fields.push(self.receipt_id.as_str().as_bytes());
        fields.push(self.context.context_digest.as_bytes());
        fields.push(self.signed_envelope.envelope_digest.as_bytes());
        fields.push(self.verification_receipt_id.as_str().as_bytes());
        fields.push(self.verification_status.as_str().as_bytes());
        fields.push(self.envelope_digest.as_bytes());
        fields.push(self.verified_context_digest.as_bytes());
        fields.push(self.verified_payload_digest.as_bytes());
        fields.push(self.verified_catalog_digest.as_bytes());
        fields.push(self.verified_method.as_str().as_bytes());
        fields.push(self.verified_method_schema_id.as_str().as_bytes());
        fields.push(self.verified_method_schema_digest.as_bytes());
        fields.push(self.signer_key_id.as_str().as_bytes());
        fields.push(verified_at);
        fields.push(verification_valid_until);
    }

    fn push_admission_fields<'a>(
        &'a self,
        fields: &mut Vec<&'a [u8]>,
        admission_identity_epoch: &'a [u8; 8],
        admission_policy_epoch: &'a [u8; 8],
        admission_issued_at: &'a [u8; 8],
        admission_expires_at: &'a [u8; 8],
    ) {
        fields.push(self.admission_receipt_id.as_str().as_bytes());
        fields.push(self.admission_decision_id.as_str().as_bytes());
        fields.push(self.admission_manifest_digest.as_bytes());
        fields.push(self.admission_catalog_digest.as_bytes());
        fields.push(self.admission_context_digest.as_bytes());
        fields.push(self.admission_operation_replay_digest.as_bytes());
        fields.push(self.admission_nonce_replay_digest.as_bytes());
        fields.push(self.admission_policy_digest.as_bytes());
        fields.push(self.admission_egress_authorization_digest.as_bytes());
        fields.push(admission_identity_epoch);
        fields.push(admission_policy_epoch);
        fields.push(self.admission_state.as_str().as_bytes());
        fields.push(admission_issued_at);
        fields.push(admission_expires_at);
    }

    fn push_decision_fields<'a>(
        &'a self,
        fields: &mut Vec<&'a [u8]>,
        decision_policy_epoch: &'a [u8; 8],
        decision_retryable: &'a [u8; 1],
    ) {
        fields.push(self.decision_id.as_str().as_bytes());
        fields.push(optional_opaque_bytes(self.parent_decision_id.as_ref()));
        fields.push(self.decision_outcome.as_str().as_bytes());
        fields.push(self.decision_context_digest.as_bytes());
        fields.push(self.decision_policy_digest.as_bytes());
        fields.push(self.decision_egress_authorization_digest.as_bytes());
        fields.push(decision_policy_epoch);
        fields.push(self.decision_reason_code.as_str().as_bytes());
        fields.push(decision_retryable);
        fields.push(self.operation_replay_digest.as_bytes());
        fields.push(self.nonce_replay_digest.as_bytes());
    }
}

fn require(invariant: bool, message: &str) -> Result<(), String> {
    if invariant {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn verification_time_is_valid(authority: &VerifiedAuthority) -> bool {
    authority.verification_valid_until > authority.verified_at
        && authority.verified_at >= authority.context.issued_at
        && authority.verification_valid_until <= authority.context.expires_at
}

fn admission_time_is_valid(authority: &VerifiedAuthority) -> bool {
    authority.admission_expires_at > authority.admission_issued_at
        && authority.admission_issued_at >= authority.verified_at
        && authority.admission_expires_at <= authority.context.expires_at
        && authority.valid_until >= authority.admission_issued_at
}

fn replay_time_is_valid(authority: &VerifiedAuthority) -> bool {
    authority.replay_receipt.recorded_at >= authority.admission_issued_at
        && authority.replay_receipt.recorded_at <= authority.valid_until
        && authority
            .replay_receipt
            .nonce_consumed_at
            .is_some_and(|consumed_at| {
                consumed_at >= authority.admission_issued_at
                    && consumed_at <= authority.admission_expires_at
            })
}

fn authority_state_is_admitted(authority: &VerifiedAuthority) -> bool {
    authority.verification_status.as_str() == "verified"
        && authority.admission_state.as_str() == "admitted"
        && authority.decision_outcome.as_str() == "allow"
        && matches!(
            authority.replay_receipt.status.as_str(),
            "consumed" | "duplicate"
        )
}

fn operation_identity_matches_context(
    authority: &VerifiedAuthority,
    operation: &OperationReplayIdentity,
    nonce: &NonceReplayKey,
) -> bool {
    operation_principal_matches(authority, operation)
        && operation_contract_matches(authority, operation)
        && nonce_identity_matches(authority, nonce)
}

fn operation_principal_matches(
    authority: &VerifiedAuthority,
    operation: &OperationReplayIdentity,
) -> bool {
    operation.tenant == authority.context.tenant
        && operation.protocol_id == authority.context.protocol_id
        && operation.catalog_digest == authority.context.catalog_digest
        && operation.actor == authority.context.actor
        && operation.audience == authority.context.audience
        && operation.authority_scope == authority.context.authority_scope
}

fn operation_contract_matches(
    authority: &VerifiedAuthority,
    operation: &OperationReplayIdentity,
) -> bool {
    operation.operation == authority.context.operation
        && operation.purpose_kind == authority.context.purpose_kind
        && operation.purpose_resource == authority.context.purpose_resource
        && operation.policy_digest == authority.context.policy_digest
        && operation.policy_revision == authority.context.policy_revision
        && operation.policy_epoch == authority.context.policy_epoch
        && Some(&operation.idempotency_key) == authority.context.idempotency_key.as_ref()
}

fn nonce_identity_matches(authority: &VerifiedAuthority, nonce: &NonceReplayKey) -> bool {
    nonce.protocol_id == authority.context.protocol_id
        && nonce.catalog_digest == authority.context.catalog_digest
        && nonce.audience == authority.context.audience
        && nonce.actor == authority.context.actor
        && nonce.tenant == authority.context.tenant
        && nonce.nonce == authority.context.nonce
}

fn signed_envelope_matches(
    authority: &VerifiedAuthority,
    operation: &OperationReplayIdentity,
) -> bool {
    authority.verified_context_digest == authority.context.context_digest
        && authority.signed_envelope.protocol_id == authority.context.protocol_id
        && authority.signed_envelope.catalog_digest == authority.context.catalog_digest
        && authority.signed_envelope.context_digest == authority.context.context_digest
        && authority.signed_envelope.payload_digest == operation.canonical_payload_digest
        && authority.signed_envelope.audience == authority.context.audience
        && authority.signed_envelope.issued_at == authority.context.issued_at
        && authority.signed_envelope.expires_at == authority.context.expires_at
        && authority.signed_envelope.signer_key_id == authority.signer_key_id
        && authority.signed_envelope.envelope_digest == authority.envelope_digest
}

fn verification_receipt_matches(
    authority: &VerifiedAuthority,
    operation: &OperationReplayIdentity,
) -> bool {
    authority.verified_catalog_digest == authority.context.catalog_digest
        && authority.verified_payload_digest == operation.canonical_payload_digest
        && authority.verified_method == operation.method
        && authority.verified_method_schema_id == operation.method_schema_id
        && authority.verified_method_schema_digest == operation.method_schema_digest
}

fn admission_decision_matches(authority: &VerifiedAuthority) -> bool {
    authority.admission_decision_id == authority.context.policy_decision_id
        && authority.decision_id == authority.context.policy_decision_id
        && authority.admission_catalog_digest == authority.context.catalog_digest
        && authority.admission_context_digest == authority.context.context_digest
}

fn admission_replay_matches(
    authority: &VerifiedAuthority,
    operation_digest: Digest256,
    nonce_digest: Digest256,
) -> bool {
    authority.admission_operation_replay_digest == operation_digest
        && authority.admission_nonce_replay_digest == nonce_digest
        && authority.admission_policy_digest == authority.context.policy_digest
        && authority.admission_egress_authorization_digest
            == authority.decision_egress_authorization_digest
        && authority.admission_durable_identity_epoch == authority.durable_identity_epoch
        && authority.admission_policy_epoch == authority.context.policy_epoch
}

fn decision_matches(authority: &VerifiedAuthority) -> bool {
    authority.decision_policy_digest == authority.context.policy_digest
        && authority.decision_policy_epoch == authority.context.policy_epoch
        && authority.decision_context_digest == authority.context.context_digest
        && authority.parent_decision_id.as_ref() != Some(&authority.decision_id)
}

fn replay_receipt_matches(
    authority: &VerifiedAuthority,
    operation_digest: Digest256,
    nonce_digest: Digest256,
) -> bool {
    authority.replay_receipt.context_digest == authority.context.context_digest
        && authority.operation_replay_digest == operation_digest
        && authority.nonce_replay_digest == nonce_digest
        && authority.replay_receipt.operation_replay_digest == operation_digest
        && authority.replay_receipt.nonce_replay_digest == nonce_digest
}

fn optional_opaque_bytes(value: Option<&OpaqueId>) -> &[u8] {
    value.map_or(b"".as_slice(), |item| item.as_str().as_bytes())
}
