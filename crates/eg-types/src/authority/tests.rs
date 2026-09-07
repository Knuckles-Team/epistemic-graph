use super::*;
use crate::contract::{
    ActorId, AudienceId, BoundedVec, Digest256, Ed25519Signature, IdempotencyKey,
    MethodId, Nonce, OpaqueId, PolicyRevision, ProtocolId, ResourceId, SchemaId,
    TenantId, UtcUnixNanos,
};

fn digest(byte: u8) -> Digest256 {
    Digest256::from_bytes([byte; 32])
}

fn context(nonce: u8, request: &str, issued_at: i64) -> AuthorityContext {
    let scope_id = ResourceId::new("graph:tenant:a/g").unwrap();
    let mut value = AuthorityContext {
        schema_version: ResourceId::new(AUTHORITY_CONTEXT_SCHEMA_V1).unwrap(),
        protocol_id: ProtocolId::new(AUTHORITY_PROTOCOL_V1).unwrap(),
        catalog_digest: digest(1),
        request_id: OpaqueId::new(request).unwrap(),
        trace_id: OpaqueId::new(format!("trace-{request}")).unwrap(),
        ingress_surface: IngressSurface::new("au_mcp").unwrap(),
        actor: ActorId::new("actor:a").unwrap(),
        audience: AudienceId::new("eg").unwrap(),
        tenant: TenantId::new("tenant:a").unwrap(),
        authority_scope: AuthorityScope {
            kind: ScopeKind::new("graph").unwrap(),
            scope_id: scope_id.clone(),
            tenant: Some(TenantId::new("tenant:a").unwrap()),
            parent_scope_ids: BoundedVec::new(vec![ResourceId::new("tenant:a").unwrap()])
                .unwrap(),
            graph_incarnation: None,
        },
        purpose_kind: PurposeKind::new("graph_write").unwrap(),
        purpose_resource: Some(scope_id),
        operation: Operation::new("mutation").unwrap(),
        policy_revision: PolicyRevision::new("policy:1").unwrap(),
        policy_epoch: 7,
        policy_decision_id: OpaqueId::new("decision:1").unwrap(),
        policy_digest: digest(2),
        issued_at: UtcUnixNanos::new(issued_at),
        expires_at: UtcUnixNanos::new(issued_at + 10_000),
        nonce: Nonce::from_bytes([nonce; 32]),
        idempotency_key: Some(IdempotencyKey::new("idem:stable").unwrap()),
        context_digest: digest(0),
    };
    value.context_digest = value.recompute_context_digest().unwrap();
    value
}

fn operation(context: &AuthorityContext, payload: Digest256) -> OperationReplayIdentity {
    OperationReplayIdentity::from_context(
        context,
        MethodId::new("mutation.apply").unwrap(),
        SchemaId::new("mutation-envelope.v1").unwrap(),
        digest(8),
        payload,
    )
    .unwrap()
}

fn verified(
    context: AuthorityContext,
    operation: &OperationReplayIdentity,
    nonce: &NonceReplayKey,
) -> VerifiedAuthority {
    let mut signed_envelope = SignedAuthorityEnvelopeEvidence {
        protocol_id: context.protocol_id.clone(),
        schema_version: ResourceId::new("signed-authority-envelope.v1").unwrap(),
        catalog_digest: context.catalog_digest,
        context_digest: context.context_digest,
        payload_digest: operation.canonical_payload_digest,
        audience: context.audience.clone(),
        issued_at: context.issued_at,
        expires_at: context.expires_at,
        signer_key_id: OpaqueId::new("signer:1").unwrap(),
        signer_key_version: 3,
        signature_algorithm: ResourceId::new("ed25519").unwrap(),
        canonicalization: ResourceId::new("canonical-msgpack.v1").unwrap(),
        unsigned_message_digest: digest(0),
        signature: Ed25519Signature::from_bytes([20; 64]),
        envelope_digest: digest(0),
    };
    signed_envelope.unsigned_message_digest =
        signed_envelope.recompute_unsigned_message_digest().unwrap();
    signed_envelope.envelope_digest = signed_envelope.recompute_envelope_digest().unwrap();
    let operation_digest = operation.digest().unwrap();
    let nonce_digest = nonce.digest().unwrap();
    let mut authority = VerifiedAuthority {
        receipt_id: OpaqueId::new("authority:1").unwrap(),
        context: context.clone(),
        signed_envelope: signed_envelope.clone(),
        verification_receipt_id: OpaqueId::new("verification:1").unwrap(),
        verification_status: VerificationStatus::new("verified").unwrap(),
        envelope_digest: signed_envelope.envelope_digest,
        verified_context_digest: context.context_digest,
        verified_payload_digest: operation.canonical_payload_digest,
        verified_catalog_digest: context.catalog_digest,
        verified_method: operation.method.clone(),
        verified_method_schema_id: operation.method_schema_id.clone(),
        verified_method_schema_digest: operation.method_schema_digest,
        signer_key_id: signed_envelope.signer_key_id.clone(),
        verified_at: UtcUnixNanos::new(110),
        verification_valid_until: UtcUnixNanos::new(9_000),
        admission_receipt_id: OpaqueId::new("admission:1").unwrap(),
        admission_decision_id: OpaqueId::new("decision:1").unwrap(),
        admission_manifest_digest: digest(21),
        admission_catalog_digest: context.catalog_digest,
        admission_context_digest: context.context_digest,
        admission_operation_replay_digest: operation_digest,
        admission_nonce_replay_digest: nonce_digest,
        admission_policy_digest: context.policy_digest,
        admission_egress_authorization_digest: digest(23),
        admission_durable_identity_epoch: 4,
        admission_policy_epoch: context.policy_epoch,
        admission_state: AdmissionOutcome::new("admitted").unwrap(),
        admission_issued_at: UtcUnixNanos::new(115),
        admission_expires_at: UtcUnixNanos::new(8_000),
        replay_receipt: ReplayReceipt {
            receipt_id: OpaqueId::new("replay:1").unwrap(),
            context_digest: context.context_digest,
            operation_replay_digest: operation_digest,
            nonce_replay_digest: nonce_digest,
            replay_ledger_epoch: Some(9),
            nonce_consumed_at: Some(UtcUnixNanos::new(120)),
            recorded_at: UtcUnixNanos::new(121),
            status: ReplayStatus::new("consumed").unwrap(),
            effect_state: EffectState::new("committed").unwrap(),
            effect_id: Some(OpaqueId::new("effect:1").unwrap()),
            commit_id: Some(OpaqueId::new("commit:1").unwrap()),
            effect_digest: Some(digest(24)),
            result_digest: Some(digest(22)),
            prior_replay_receipt_id: None,
            prior_effect_id: None,
            prior_commit_id: None,
            prior_effect_digest: None,
            prior_result_digest: None,
            retryable: false,
        },
        valid_until: UtcUnixNanos::new(8_000),
        durable_identity_epoch: 4,
        decision_id: OpaqueId::new("decision:1").unwrap(),
        parent_decision_id: None,
        decision_outcome: DecisionOutcome::new("allow").unwrap(),
        decision_context_digest: context.context_digest,
        decision_policy_digest: context.policy_digest,
        decision_egress_authorization_digest: digest(23),
        decision_policy_epoch: context.policy_epoch,
        decision_reason_code: ResourceId::new("policy:allow").unwrap(),
        decision_retryable: false,
        operation_replay_digest: operation_digest,
        nonce_replay_digest: nonce_digest,
        evidence_digest: digest(0),
    };
    authority.evidence_digest = authority.recompute_evidence_digest().unwrap();
    authority
}

#[test]
fn fresh_attempt_changes_context_and_nonce_but_not_operation_identity() {
    let first = context(3, "request:1", 100);
    let second = context(4, "request:2", 200);
    let payload = digest(9);
    let first_operation = operation(&first, payload);
    let second_operation = operation(&second, payload);
    assert_eq!(
        first_operation.digest().unwrap(),
        second_operation.digest().unwrap()
    );
    assert_ne!(first.context_digest, second.context_digest);
    assert_ne!(
        NonceReplayKey::from_context(&first)
            .unwrap()
            .digest()
            .unwrap(),
        NonceReplayKey::from_context(&second)
            .unwrap()
            .digest()
            .unwrap()
    );
}

#[test]
fn changing_payload_method_scope_or_policy_conflicts() {
    let base = context(3, "request:1", 100);
    let base_identity = operation(&base, digest(9));
    let base_digest = base_identity.digest().unwrap();

    let changed_payload = OperationReplayIdentity {
        canonical_payload_digest: digest(8),
        ..base_identity.clone()
    };
    let changed_method = OperationReplayIdentity {
        method: MethodId::new("graph.node.delete").unwrap(),
        ..base_identity.clone()
    };
    let changed_policy = OperationReplayIdentity {
        policy_digest: digest(7),
        ..base_identity.clone()
    };
    let changed_scope = OperationReplayIdentity {
        authority_scope: AuthorityScope {
            kind: ScopeKind::new("graph").unwrap(),
            scope_id: ResourceId::new("graph:tenant:a/other").unwrap(),
            tenant: Some(TenantId::new("tenant:a").unwrap()),
            parent_scope_ids: BoundedVec::new(vec![ResourceId::new("tenant:a").unwrap()])
                .unwrap(),
            graph_incarnation: None,
        },
        purpose_resource: Some(ResourceId::new("graph:tenant:a/other").unwrap()),
        ..base_identity
    };
    for changed in [
        changed_payload,
        changed_method,
        changed_policy,
        changed_scope,
    ] {
        assert_ne!(base_digest, changed.digest().unwrap());
    }
}

#[test]
fn scope_tenant_target_and_operation_purpose_are_exact() {
    let mut value = context(3, "request:1", 100);
    value.authority_scope.tenant = Some(TenantId::new("tenant:b").unwrap());
    assert!(value.validate().is_err());

    let mut value = context(3, "request:1", 100);
    value.purpose_kind = PurposeKind::new("source_ingest").unwrap();
    assert!(value.recompute_context_digest().is_err());
}

#[test]
fn replay_receipt_matrix_rejects_cross_state_fields() {
    let context = context(3, "request:1", 100);
    let operation = operation(&context, digest(9));
    let nonce = NonceReplayKey::from_context(&context).unwrap();
    let mut authority = verified(context, &operation, &nonce);
    assert!(authority
        .validate_evidence_bindings(&operation, &nonce)
        .is_ok());
    authority.replay_receipt.status = ReplayStatus::new("duplicate").unwrap();
    assert!(authority.replay_receipt.validate().is_err());

    authority.replay_receipt.prior_result_digest = authority.replay_receipt.result_digest;
    authority.replay_receipt.prior_effect_id = authority.replay_receipt.effect_id.take();
    authority.replay_receipt.prior_commit_id = authority.replay_receipt.commit_id.take();
    authority.replay_receipt.prior_effect_digest = authority.replay_receipt.effect_digest.take();
    authority.replay_receipt.prior_replay_receipt_id =
        Some(authority.replay_receipt.receipt_id.clone());
    assert!(authority.replay_receipt.validate().is_err());
    authority.replay_receipt.prior_replay_receipt_id =
        Some(OpaqueId::new("replay:prior").unwrap());
    assert!(authority.replay_receipt.validate().is_ok());
}

#[test]
fn authority_envelope_mismatch_is_rejected_even_when_self_consistent() {
    let context = context(3, "request:1", 100);
    let operation = operation(&context, digest(9));
    let nonce = NonceReplayKey::from_context(&context).unwrap();
    let mut authority = verified(context, &operation, &nonce);
    authority.signed_envelope.payload_digest = digest(99);
    authority.signed_envelope.envelope_digest = authority
        .signed_envelope
        .recompute_envelope_digest()
        .unwrap();
    authority.envelope_digest = authority.signed_envelope.envelope_digest;
    assert!(authority
        .validate_evidence_bindings(&operation, &nonce)
        .is_err());

    let mut authority = verified(authority.context.clone(), &operation, &nonce);
    authority.parent_decision_id = Some(authority.decision_id.clone());
    authority.evidence_digest = authority.recompute_evidence_digest().unwrap();
    assert!(authority
        .validate_evidence_bindings(&operation, &nonce)
        .is_err());

    let mut authority = verified(authority.context.clone(), &operation, &nonce);
    authority.signed_envelope.signature = Ed25519Signature::from_bytes([99; 64]);
    assert!(authority.signed_envelope.validate().is_err());
}

#[test]
fn authority_evidence_digest_binds_admission_fields_and_time_order() {
    let context = context(3, "request:1", 100);
    let operation = operation(&context, digest(9));
    let nonce = NonceReplayKey::from_context(&context).unwrap();
    let mut authority = verified(context, &operation, &nonce);
    authority.admission_manifest_digest = digest(77);
    assert!(authority
        .validate_evidence_bindings(&operation, &nonce)
        .is_err());

    let mut authority = verified(authority.context.clone(), &operation, &nonce);
    let spliced = OpaqueId::new("decision:unrelated").unwrap();
    authority.admission_decision_id = spliced.clone();
    authority.decision_id = spliced;
    authority.evidence_digest = authority.recompute_evidence_digest().unwrap();
    assert!(authority
        .validate_evidence_bindings(&operation, &nonce)
        .is_err());

    let mut authority = verified(authority.context.clone(), &operation, &nonce);
    authority.replay_receipt.nonce_consumed_at = Some(UtcUnixNanos::new(114));
    authority.replay_receipt.recorded_at = UtcUnixNanos::new(116);
    authority.evidence_digest = authority.recompute_evidence_digest().unwrap();
    assert!(authority
        .validate_evidence_bindings(&operation, &nonce)
        .is_err());
}

#[test]
fn extract_and_audit_require_stable_idempotency() {
    for (operation, purpose) in [("extract", "source_extract"), ("audit", "audit")] {
        let mut value = context(3, "request:1", 100);
        value.operation = Operation::new(operation).unwrap();
        value.purpose_kind = PurposeKind::new(purpose).unwrap();
        value.idempotency_key = None;
        value.context_digest = value.recompute_context_digest().unwrap();
        assert!(value.validate().is_err());
    }
}
