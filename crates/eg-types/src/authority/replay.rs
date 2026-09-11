//! Stable operation identity, nonce identity, and durable replay evidence.

use serde::{Deserialize, Serialize};

use super::context::{
    digest_purpose, validate_operation_purpose, AuthorityContext, AuthorityScope,
    AUTHORITY_PROTOCOL_V1,
};
use crate::contract::{
    ActorId, AudienceId, Digest256, EffectState, IdempotencyKey, MethodId, Nonce,
    OpaqueId, Operation, PolicyRevision, ProtocolId, PurposeKind, ReplayStatus,
    ResourceId, SchemaId, TenantId, UtcUnixNanos,
};

/// Stable effect identity. The type cannot contain nonce, request/trace IDs, or
/// timestamps. A fresh attempt reconstructs the same value from its admitted
/// context when the semantically stable fields are identical.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OperationReplayIdentity {
    pub schema_version: ResourceId,
    pub protocol_id: ProtocolId,
    pub catalog_digest: Digest256,
    pub tenant: TenantId,
    pub actor: ActorId,
    pub audience: AudienceId,
    pub authority_scope: AuthorityScope,
    pub operation: Operation,
    pub purpose_kind: PurposeKind,
    pub purpose_resource: Option<ResourceId>,
    pub method: MethodId,
    pub method_schema_id: SchemaId,
    pub method_schema_digest: Digest256,
    pub canonical_payload_digest: Digest256,
    pub policy_revision: PolicyRevision,
    pub policy_epoch: u64,
    pub policy_digest: Digest256,
    pub idempotency_key: IdempotencyKey,
}

impl OperationReplayIdentity {
    pub fn from_context(
        context: &AuthorityContext,
        method: MethodId,
        method_schema_id: SchemaId,
        method_schema_digest: Digest256,
        canonical_payload_digest: Digest256,
    ) -> Result<Self, String> {
        context.validate()?;
        Ok(Self {
            schema_version: ResourceId::new("operation-replay-identity.v1")?,
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

    pub fn digest(&self) -> Result<Digest256, String> {
        self.validate()?;
        let scope_digest = self.authority_scope.digest()?;
        let purpose_digest = digest_purpose(
            &self.operation,
            &self.purpose_kind,
            self.purpose_resource.as_ref(),
            &self.authority_scope,
        )?;
        let policy_epoch = self.policy_epoch.to_be_bytes();
        Digest256::framed(
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
/// `OperationReplayIdentity` remains stable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct NonceReplayKey {
    pub protocol_id: ProtocolId,
    pub catalog_digest: Digest256,
    pub audience: AudienceId,
    pub actor: ActorId,
    pub tenant: TenantId,
    pub nonce: Nonce,
}

impl NonceReplayKey {
    pub fn from_context(context: &AuthorityContext) -> Result<Self, String> {
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

    pub fn digest(&self) -> Result<Digest256, String> {
        if self.protocol_id.as_str() != AUTHORITY_PROTOCOL_V1 {
            return Err("nonce replay key protocol is invalid".into());
        }
        Digest256::framed(
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
pub struct ReplayReceipt {
    pub receipt_id: OpaqueId,
    pub context_digest: Digest256,
    pub operation_replay_digest: Digest256,
    pub nonce_replay_digest: Digest256,
    pub replay_ledger_epoch: Option<u64>,
    pub nonce_consumed_at: Option<UtcUnixNanos>,
    pub recorded_at: UtcUnixNanos,
    pub status: ReplayStatus,
    pub effect_state: EffectState,
    pub effect_id: Option<OpaqueId>,
    pub commit_id: Option<OpaqueId>,
    pub effect_digest: Option<Digest256>,
    pub result_digest: Option<Digest256>,
    pub prior_replay_receipt_id: Option<OpaqueId>,
    pub prior_effect_id: Option<OpaqueId>,
    pub prior_commit_id: Option<OpaqueId>,
    pub prior_effect_digest: Option<Digest256>,
    pub prior_result_digest: Option<Digest256>,
    pub retryable: bool,
}

impl ReplayReceipt {
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

    pub fn evidence_digest(&self) -> Result<Digest256, String> {
        self.validate()?;
        let ledger_present = [u8::from(self.replay_ledger_epoch.is_some())];
        let ledger_epoch = self.replay_ledger_epoch.unwrap_or(0).to_be_bytes();
        let consumed_present = [u8::from(self.nonce_consumed_at.is_some())];
        let consumed_at = self
            .nonce_consumed_at
            .map_or(0, UtcUnixNanos::get)
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
        Digest256::framed(b"eg/replay-receipt-evidence/v1", &fields)
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

fn consumed_effect_is_bound(receipt: &ReplayReceipt) -> bool {
    receipt.replay_ledger_epoch.is_some()
        && receipt.nonce_consumed_at.is_some()
        && receipt
            .nonce_consumed_at
            .is_some_and(|consumed_at| consumed_at <= receipt.recorded_at)
        && receipt.effect_id.is_some()
}

fn current_effect_is_absent(receipt: &ReplayReceipt) -> bool {
    receipt.effect_id.is_none() && receipt.commit_id.is_none() && receipt.effect_digest.is_none()
}

fn prior_effect_is_absent(receipt: &ReplayReceipt) -> bool {
    receipt.prior_replay_receipt_id.is_none()
        && receipt.prior_effect_id.is_none()
        && receipt.prior_commit_id.is_none()
        && receipt.prior_effect_digest.is_none()
        && receipt.prior_result_digest.is_none()
}

fn replay_pending(receipt: &ReplayReceipt) -> bool {
    consumed_effect_is_bound(receipt)
        && receipt.commit_id.is_none()
        && receipt.effect_digest.is_none()
        && receipt.result_digest.is_none()
        && prior_effect_is_absent(receipt)
        && receipt.retryable
}

fn replay_committed(receipt: &ReplayReceipt) -> bool {
    consumed_effect_is_bound(receipt)
        && receipt.commit_id.is_some()
        && receipt.effect_digest.is_some()
        && receipt.result_digest.is_some()
        && prior_effect_is_absent(receipt)
        && !receipt.retryable
}

fn replay_failure(receipt: &ReplayReceipt, retryable: bool) -> bool {
    consumed_effect_is_bound(receipt)
        && receipt.commit_id.is_none()
        && receipt.effect_digest.is_none()
        && receipt.result_digest.is_none()
        && prior_effect_is_absent(receipt)
        && receipt.retryable == retryable
}

fn replay_duplicate(receipt: &ReplayReceipt) -> bool {
    receipt.replay_ledger_epoch.is_some()
        && receipt
            .nonce_consumed_at
            .is_some_and(|consumed_at| consumed_at <= receipt.recorded_at)
        && current_effect_is_absent(receipt)
        && receipt.result_digest.is_some()
        && duplicate_prior_is_bound(receipt)
        && !receipt.retryable
}

fn duplicate_prior_is_bound(receipt: &ReplayReceipt) -> bool {
    receipt.prior_replay_receipt_id.is_some()
        && receipt.prior_effect_id.is_some()
        && receipt.prior_commit_id.is_some()
        && receipt.prior_effect_digest.is_some()
        && receipt.prior_replay_receipt_id.as_ref() != Some(&receipt.receipt_id)
        && receipt.prior_result_digest == receipt.result_digest
}

fn replay_rejected(receipt: &ReplayReceipt) -> bool {
    receipt.replay_ledger_epoch.is_some()
        && receipt.nonce_consumed_at.is_none()
        && current_effect_is_absent(receipt)
        && receipt.result_digest.is_none()
        && prior_effect_is_absent(receipt)
        && !receipt.retryable
}

fn replay_unavailable(receipt: &ReplayReceipt) -> bool {
    receipt.replay_ledger_epoch.is_none()
        && receipt.nonce_consumed_at.is_none()
        && current_effect_is_absent(receipt)
        && receipt.result_digest.is_none()
        && prior_effect_is_absent(receipt)
        && receipt.retryable
}

fn optional_opaque_bytes(value: Option<&OpaqueId>) -> &[u8] {
    value.map_or(b"".as_slice(), |item| item.as_str().as_bytes())
}

fn optional_digest_bytes(value: Option<&Digest256>) -> &[u8] {
    value.map_or(b"".as_slice(), |item| item.as_bytes().as_slice())
}
