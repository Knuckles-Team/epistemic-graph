//! Stable operation identity, nonce identity, and durable replay evidence.

use serde::{Deserialize, Serialize};

use super::context::{
    digest_purpose, validate_operation_purpose, AuthorityContextV1, AuthorityScopeV1,
    AUTHORITY_PROTOCOL_V1,
};
use crate::contract::{
    ActorIdV1, AudienceIdV1, Digest256V1, EffectStateV1, IdempotencyKeyV1, MethodIdV1, NonceV1,
    OpaqueIdV1, OperationV1, PolicyRevisionV1, ProtocolIdV1, PurposeKindV1, ReplayStatusV1,
    ResourceIdV1, SchemaIdV1, TenantIdV1, UtcUnixNanosV1,
};

/// Stable effect identity. The type cannot contain nonce, request/trace IDs, or
/// timestamps. A fresh attempt reconstructs the same value from its admitted
/// context when the semantically stable fields are identical.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationReplayIdentityV1 {
    pub schema_version: ResourceIdV1,
    pub protocol_id: ProtocolIdV1,
    pub catalog_digest: Digest256V1,
    pub tenant: TenantIdV1,
    pub actor: ActorIdV1,
    pub audience: AudienceIdV1,
    pub authority_scope: AuthorityScopeV1,
    pub operation: OperationV1,
    pub purpose_kind: PurposeKindV1,
    pub purpose_resource: Option<ResourceIdV1>,
    pub method: MethodIdV1,
    pub method_schema_id: SchemaIdV1,
    pub method_schema_digest: Digest256V1,
    pub canonical_payload_digest: Digest256V1,
    pub policy_revision: PolicyRevisionV1,
    pub policy_epoch: u64,
    pub policy_digest: Digest256V1,
    pub idempotency_key: IdempotencyKeyV1,
}

impl OperationReplayIdentityV1 {
    pub fn from_context(
        context: &AuthorityContextV1,
        method: MethodIdV1,
        method_schema_id: SchemaIdV1,
        method_schema_digest: Digest256V1,
        canonical_payload_digest: Digest256V1,
    ) -> Result<Self, String> {
        context.validate()?;
        Ok(Self {
            schema_version: ResourceIdV1::new("operation-replay-identity.v1")?,
            protocol_id: context.protocol_id.clone(),
            catalog_digest: context.catalog_digest,
            tenant: context.tenant.clone(),
            actor: context.actor.clone(),
            audience: context.audience.clone(),
            authority_scope: context.authority_scope.clone(),
            operation: context.operation.clone(),
            purpose_kind: context.purpose_kind.clone(),
            purpose_resource: context.purpose_resource.clone(),
            method,
            method_schema_id,
            method_schema_digest,
            canonical_payload_digest,
            policy_revision: context.policy_revision.clone(),
            policy_epoch: context.policy_epoch,
            policy_digest: context.policy_digest,
            idempotency_key: context
                .idempotency_key
                .clone()
                .ok_or_else(|| "effectful operation requires an idempotency key".to_string())?,
        })
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version.as_str() != "operation-replay-identity.v1"
            || self.protocol_id.as_str() != AUTHORITY_PROTOCOL_V1
        {
            return Err("operation replay identity schema or protocol is invalid".into());
        }
        self.authority_scope.validate()?;
        if self.authority_scope.tenant.as_ref() != Some(&self.tenant) {
            return Err("operation replay scope tenant differs from operation tenant".into());
        }
        validate_operation_purpose(
            &self.operation,
            &self.purpose_kind,
            self.purpose_resource.as_ref(),
            &self.authority_scope,
        )
    }

    pub fn digest(&self) -> Result<Digest256V1, String> {
        self.validate()?;
        let scope_digest = self.authority_scope.digest()?;
        let purpose_digest = digest_purpose(
            &self.operation,
            &self.purpose_kind,
            self.purpose_resource.as_ref(),
            &self.authority_scope,
        )?;
        let policy_epoch = self.policy_epoch.to_be_bytes();
        Digest256V1::framed(
            b"eg/operation-replay-identity/v1",
            &[
                self.schema_version.as_str().as_bytes(),
                self.protocol_id.as_str().as_bytes(),
                self.catalog_digest.as_bytes(),
                self.tenant.as_str().as_bytes(),
                self.actor.as_str().as_bytes(),
                self.audience.as_str().as_bytes(),
                scope_digest.as_bytes(),
                self.operation.as_str().as_bytes(),
                purpose_digest.as_bytes(),
                self.method.as_str().as_bytes(),
                self.method_schema_id.as_str().as_bytes(),
                self.method_schema_digest.as_bytes(),
                self.canonical_payload_digest.as_bytes(),
                self.policy_revision.as_str().as_bytes(),
                &policy_epoch,
                self.policy_digest.as_bytes(),
                self.idempotency_key.as_str().as_bytes(),
            ],
        )
    }
}

/// Attempt-specific nonce key. Its digest changes for a fresh nonce even when
/// `OperationReplayIdentityV1` remains stable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NonceReplayKeyV1 {
    pub protocol_id: ProtocolIdV1,
    pub catalog_digest: Digest256V1,
    pub audience: AudienceIdV1,
    pub actor: ActorIdV1,
    pub tenant: TenantIdV1,
    pub nonce: NonceV1,
}

impl NonceReplayKeyV1 {
    pub fn from_context(context: &AuthorityContextV1) -> Result<Self, String> {
        context.validate()?;
        Ok(Self {
            protocol_id: context.protocol_id.clone(),
            catalog_digest: context.catalog_digest,
            audience: context.audience.clone(),
            actor: context.actor.clone(),
            tenant: context.tenant.clone(),
            nonce: context.nonce,
        })
    }

    pub fn digest(&self) -> Result<Digest256V1, String> {
        if self.protocol_id.as_str() != AUTHORITY_PROTOCOL_V1 {
            return Err("nonce replay key protocol is invalid".into());
        }
        Digest256V1::framed(
            b"eg/nonce-replay-key/v1",
            &[
                self.protocol_id.as_str().as_bytes(),
                self.catalog_digest.as_bytes(),
                self.audience.as_str().as_bytes(),
                self.actor.as_str().as_bytes(),
                self.tenant.as_str().as_bytes(),
                self.nonce.as_bytes(),
            ],
        )
    }
}

/// Durable replay evidence binds the stable operation and attempt-specific nonce
/// digests separately. It never infers either one from `context_digest`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayReceiptV1 {
    pub receipt_id: OpaqueIdV1,
    pub context_digest: Digest256V1,
    pub operation_replay_digest: Digest256V1,
    pub nonce_replay_digest: Digest256V1,
    pub replay_ledger_epoch: Option<u64>,
    pub nonce_consumed_at: Option<UtcUnixNanosV1>,
    pub recorded_at: UtcUnixNanosV1,
    pub status: ReplayStatusV1,
    pub effect_state: EffectStateV1,
    pub effect_id: Option<OpaqueIdV1>,
    pub commit_id: Option<OpaqueIdV1>,
    pub effect_digest: Option<Digest256V1>,
    pub result_digest: Option<Digest256V1>,
    pub prior_replay_receipt_id: Option<OpaqueIdV1>,
    pub prior_effect_id: Option<OpaqueIdV1>,
    pub prior_commit_id: Option<OpaqueIdV1>,
    pub prior_effect_digest: Option<Digest256V1>,
    pub prior_result_digest: Option<Digest256V1>,
    pub retryable: bool,
}

impl ReplayReceiptV1 {
    pub fn validate(&self) -> Result<(), String> {
        let valid = match (self.status.as_str(), self.effect_state.as_str()) {
            ("consumed", "pending") => replay_pending(self),
            ("consumed", "committed") => replay_committed(self),
            ("consumed", "failed_retryable") => replay_failure(self, true),
            ("consumed", "failed_terminal") => replay_failure(self, false),
            ("duplicate", "committed") => replay_duplicate(self),
            ("rejected", "none") => replay_rejected(self),
            ("unavailable", "none") => replay_unavailable(self),
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err("replay receipt status/effect/result/time/epoch matrix is invalid".into())
        }
    }

    pub fn evidence_digest(&self) -> Result<Digest256V1, String> {
        self.validate()?;
        let ledger_present = [u8::from(self.replay_ledger_epoch.is_some())];
        let ledger_epoch = self.replay_ledger_epoch.unwrap_or(0).to_be_bytes();
        let consumed_present = [u8::from(self.nonce_consumed_at.is_some())];
        let consumed_at = self
            .nonce_consumed_at
            .map_or(0, UtcUnixNanosV1::get)
            .to_be_bytes();
        let recorded_at = self.recorded_at.get().to_be_bytes();
        let retryable = [u8::from(self.retryable)];
        let mut fields: Vec<&[u8]> = Vec::with_capacity(22);
        fields.push(self.receipt_id.as_str().as_bytes());
        fields.push(self.context_digest.as_bytes());
        fields.push(self.operation_replay_digest.as_bytes());
        fields.push(self.nonce_replay_digest.as_bytes());
        fields.push(&ledger_present);
        fields.push(&ledger_epoch);
        fields.push(&consumed_present);
        fields.push(&consumed_at);
        fields.push(&recorded_at);
        fields.push(self.status.as_str().as_bytes());
        fields.push(self.effect_state.as_str().as_bytes());
        self.push_effect_fields(&mut fields);
        fields.push(&retryable);
        Digest256V1::framed(b"eg/replay-receipt-evidence/v1", &fields)
    }

    fn push_effect_fields<'a>(&'a self, fields: &mut Vec<&'a [u8]>) {
        fields.push(optional_opaque_bytes(self.effect_id.as_ref()));
        fields.push(optional_opaque_bytes(self.commit_id.as_ref()));
        fields.push(optional_digest_bytes(self.effect_digest.as_ref()));
        fields.push(optional_digest_bytes(self.result_digest.as_ref()));
        fields.push(optional_opaque_bytes(self.prior_replay_receipt_id.as_ref()));
        fields.push(optional_opaque_bytes(self.prior_effect_id.as_ref()));
        fields.push(optional_opaque_bytes(self.prior_commit_id.as_ref()));
        fields.push(optional_digest_bytes(self.prior_effect_digest.as_ref()));
        fields.push(optional_digest_bytes(self.prior_result_digest.as_ref()));
    }
}

fn consumed_effect_is_bound(receipt: &ReplayReceiptV1) -> bool {
    receipt.replay_ledger_epoch.is_some()
        && receipt.nonce_consumed_at.is_some()
        && receipt
            .nonce_consumed_at
            .is_some_and(|consumed_at| consumed_at <= receipt.recorded_at)
        && receipt.effect_id.is_some()
}

fn current_effect_is_absent(receipt: &ReplayReceiptV1) -> bool {
    receipt.effect_id.is_none() && receipt.commit_id.is_none() && receipt.effect_digest.is_none()
}

fn prior_effect_is_absent(receipt: &ReplayReceiptV1) -> bool {
    receipt.prior_replay_receipt_id.is_none()
        && receipt.prior_effect_id.is_none()
        && receipt.prior_commit_id.is_none()
        && receipt.prior_effect_digest.is_none()
        && receipt.prior_result_digest.is_none()
}

fn replay_pending(receipt: &ReplayReceiptV1) -> bool {
    consumed_effect_is_bound(receipt)
        && receipt.commit_id.is_none()
        && receipt.effect_digest.is_none()
        && receipt.result_digest.is_none()
        && prior_effect_is_absent(receipt)
        && receipt.retryable
}

fn replay_committed(receipt: &ReplayReceiptV1) -> bool {
    consumed_effect_is_bound(receipt)
        && receipt.commit_id.is_some()
        && receipt.effect_digest.is_some()
        && receipt.result_digest.is_some()
        && prior_effect_is_absent(receipt)
        && !receipt.retryable
}

fn replay_failure(receipt: &ReplayReceiptV1, retryable: bool) -> bool {
    consumed_effect_is_bound(receipt)
        && receipt.commit_id.is_none()
        && receipt.effect_digest.is_none()
        && receipt.result_digest.is_none()
        && prior_effect_is_absent(receipt)
        && receipt.retryable == retryable
}

fn replay_duplicate(receipt: &ReplayReceiptV1) -> bool {
    receipt.replay_ledger_epoch.is_some()
        && receipt
            .nonce_consumed_at
            .is_some_and(|consumed_at| consumed_at <= receipt.recorded_at)
        && current_effect_is_absent(receipt)
        && receipt.result_digest.is_some()
        && duplicate_prior_is_bound(receipt)
        && !receipt.retryable
}

fn duplicate_prior_is_bound(receipt: &ReplayReceiptV1) -> bool {
    receipt.prior_replay_receipt_id.is_some()
        && receipt.prior_effect_id.is_some()
        && receipt.prior_commit_id.is_some()
        && receipt.prior_effect_digest.is_some()
        && receipt.prior_replay_receipt_id.as_ref() != Some(&receipt.receipt_id)
        && receipt.prior_result_digest == receipt.result_digest
}

fn replay_rejected(receipt: &ReplayReceiptV1) -> bool {
    receipt.replay_ledger_epoch.is_some()
        && receipt.nonce_consumed_at.is_none()
        && current_effect_is_absent(receipt)
        && receipt.result_digest.is_none()
        && prior_effect_is_absent(receipt)
        && !receipt.retryable
}

fn replay_unavailable(receipt: &ReplayReceiptV1) -> bool {
    receipt.replay_ledger_epoch.is_none()
        && receipt.nonce_consumed_at.is_none()
        && current_effect_is_absent(receipt)
        && receipt.result_digest.is_none()
        && prior_effect_is_absent(receipt)
        && receipt.retryable
}

fn optional_opaque_bytes(value: Option<&OpaqueIdV1>) -> &[u8] {
    value.map_or(b"".as_slice(), |item| item.as_str().as_bytes())
}

fn optional_digest_bytes(value: Option<&Digest256V1>) -> &[u8] {
    value.map_or(b"".as_slice(), |item| item.as_bytes().as_slice())
}
