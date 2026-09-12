use super::budget::{BudgetedVecSeed, StructuralBudget};
use super::payload::recompute_egress_authorization_digest;
use super::receipt::result_matches_request;
use super::*;
use serde::de::DeserializeSeed;
use serde::ser::SerializeMap;
use serde::Serialize;

use crate::authority::{
    AdmissionOutcome, AuthorityContext, AuthorityScope, DecisionOutcome, EffectState,
    IngressSurface, NonceReplayKey, Operation, OperationReplayIdentity, ReplayReceipt,
    ReplayStatus, ScopeKind, SignedAuthorityEnvelopeEvidence, VerificationStatus,
    VerifiedAuthority, AUTHORITY_CONTEXT_SCHEMA_V1, AUTHORITY_PROTOCOL_V1,
};
use crate::contract::{
    ActorId, AudienceId, BoundedVec, Digest256, Ed25519Signature, IdempotencyKey, MethodId, Nonce,
    OpaqueId, PolicyRevision, ProtocolId, RecordBytes, ResourceId, SchemaId, TenantId,
    UtcUnixNanos, MAX_MUTATION_EFFECTS, MAX_MUTATION_ENVELOPE_BYTES, MAX_OUTBOX_INTENTS,
    MAX_SCOPE_COMPONENTS,
};
use crate::outbox::{OutboxHeader, OutboxIntent};

fn digest(byte: u8) -> Digest256 {
    Digest256::from_bytes([byte; 32])
}

fn scope() -> AuthorityScope {
    AuthorityScope {
        kind: ScopeKind::new("graph").unwrap(),
        scope_id: ResourceId::new("graph:tenant:a/g").unwrap(),
        tenant: Some(TenantId::new("tenant:a").unwrap()),
        parent_scope_ids: BoundedVec::<ResourceId, MAX_SCOPE_COMPONENTS>::new(vec![
            ResourceId::new("tenant:a").unwrap(),
        ])
        .unwrap(),
        graph_incarnation: None,
    }
}

fn intent(topic: &str) -> OutboxIntent {
    let payload = RecordBytes::new(vec![1, 2, 3]).unwrap();
    OutboxIntent {
        tenant: TenantId::new("tenant:a").unwrap(),
        destination_scope_digest: scope().digest().unwrap(),
        topic: ResourceId::new(topic).unwrap(),
        partition_key: ResourceId::new("partition:a").unwrap(),
        event_schema: ResourceId::new("event.v1").unwrap(),
        payload_digest: payload.digest().unwrap(),
        payload,
        headers: BoundedVec::new(vec![]).unwrap(),
    }
}

fn delete_effect(ordinal: u32) -> MutationEffect {
    MutationEffect {
        ordinal,
        target: RecordTarget {
            tenant: TenantId::new("tenant:a").unwrap(),
            scope_digest: scope().digest().unwrap(),
            domain: MutationDomain::new("graph").unwrap(),
            schema_id: SchemaId::new("node.v1").unwrap(),
            record_id: ResourceId::new(format!("node:{ordinal}")).unwrap(),
        },
        mutation: RecordMutation::Delete {
            expected_record_digest: digest(30),
        },
    }
}

fn authority_for(
    canonical_payload_digest: Digest256,
    egress_authorization_digest: Digest256,
    effect_digest: Digest256,
) -> (VerifiedAuthority, OperationReplayIdentity, NonceReplayKey) {
    let mut context = AuthorityContext {
        schema_version: ResourceId::new(AUTHORITY_CONTEXT_SCHEMA_V1).unwrap(),
        protocol_id: ProtocolId::new(AUTHORITY_PROTOCOL_V1).unwrap(),
        catalog_digest: digest(1),
        request_id: OpaqueId::new("request:1").unwrap(),
        trace_id: OpaqueId::new("trace:1").unwrap(),
        ingress_surface: IngressSurface::new("au_mcp").unwrap(),
        actor: ActorId::new("actor:a").unwrap(),
        audience: AudienceId::new("eg").unwrap(),
        tenant: TenantId::new("tenant:a").unwrap(),
        authority_scope: scope(),
        purpose_kind: crate::authority::PurposeKind::new("graph_write").unwrap(),
        purpose_resource: Some(scope().scope_id),
        operation: Operation::new("mutation").unwrap(),
        policy_revision: PolicyRevision::new("policy:1").unwrap(),
        policy_epoch: 7,
        policy_decision_id: OpaqueId::new("decision:1").unwrap(),
        policy_digest: digest(2),
        issued_at: UtcUnixNanos::new(100),
        expires_at: UtcUnixNanos::new(10_100),
        nonce: Nonce::from_bytes([3; 32]),
        idempotency_key: Some(IdempotencyKey::new("idem:stable").unwrap()),
        context_digest: digest(0),
    };
    context.context_digest = context.recompute_context_digest().unwrap();
    let operation = OperationReplayIdentity::from_context(
        &context,
        MethodId::new("mutation.apply").unwrap(),
        SchemaId::new(MUTATION_ENVELOPE_SCHEMA_V1).unwrap(),
        digest(8),
        canonical_payload_digest,
    )
    .unwrap();
    let nonce = NonceReplayKey::from_context(&context).unwrap();
    let mut signed_envelope = SignedAuthorityEnvelopeEvidence {
        protocol_id: context.protocol_id.clone(),
        schema_version: ResourceId::new("signed-authority-envelope.v1").unwrap(),
        catalog_digest: context.catalog_digest,
        context_digest: context.context_digest,
        payload_digest: canonical_payload_digest,
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
    let result_digest = MutationResult::ReceiptOnly.digest().unwrap();
    let mut authority = VerifiedAuthority {
        receipt_id: OpaqueId::new("authority:1").unwrap(),
        context: context.clone(),
        signed_envelope: signed_envelope.clone(),
        verification_receipt_id: OpaqueId::new("verification:1").unwrap(),
        verification_status: VerificationStatus::new("verified").unwrap(),
        envelope_digest: signed_envelope.envelope_digest,
        verified_context_digest: context.context_digest,
        verified_payload_digest: canonical_payload_digest,
        verified_catalog_digest: context.catalog_digest,
        verified_method: operation.method.clone(),
        verified_method_schema_id: operation.method_schema_id.clone(),
        verified_method_schema_digest: operation.method_schema_digest,
        signer_key_id: signed_envelope.signer_key_id.clone(),
        verified_at: UtcUnixNanos::new(110),
        verification_valid_until: UtcUnixNanos::new(9_000),
        admission_receipt_id: OpaqueId::new("admission:1").unwrap(),
        admission_decision_id: context.policy_decision_id.clone(),
        admission_manifest_digest: digest(21),
        admission_catalog_digest: context.catalog_digest,
        admission_context_digest: context.context_digest,
        admission_operation_replay_digest: operation_digest,
        admission_nonce_replay_digest: nonce_digest,
        admission_policy_digest: context.policy_digest,
        admission_egress_authorization_digest: egress_authorization_digest,
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
            effect_digest: Some(effect_digest),
            result_digest: Some(result_digest),
            prior_replay_receipt_id: None,
            prior_effect_id: None,
            prior_commit_id: None,
            prior_effect_digest: None,
            prior_result_digest: None,
            retryable: false,
        },
        valid_until: UtcUnixNanos::new(8_000),
        durable_identity_epoch: 4,
        decision_id: context.policy_decision_id.clone(),
        parent_decision_id: None,
        decision_outcome: DecisionOutcome::new("allow").unwrap(),
        decision_context_digest: context.context_digest,
        decision_policy_digest: context.policy_digest,
        decision_egress_authorization_digest: egress_authorization_digest,
        decision_policy_epoch: context.policy_epoch,
        decision_reason_code: ResourceId::new("policy:allow").unwrap(),
        decision_retryable: false,
        operation_replay_digest: operation_digest,
        nonce_replay_digest: nonce_digest,
        evidence_digest: digest(0),
    };
    authority.evidence_digest = authority.recompute_evidence_digest().unwrap();
    (authority, operation, nonce)
}

fn envelope_with_effect_count(effect_count: usize) -> MutationRequestEnvelope {
    let effects = (0..effect_count)
        .map(|ordinal| delete_effect(u32::try_from(ordinal).unwrap()))
        .collect();
    let payload = MutationPayload::new(
        OpaqueId::new("mutation:1").unwrap(),
        scope(),
        BoundedVec::new(vec![]).unwrap(),
        BoundedVec::new(effects).unwrap(),
        BoundedVec::new(vec![intent("topic:a")]).unwrap(),
        BoundedVec::new(vec![]).unwrap(),
        RequestedMutationResult::new("receipt_only").unwrap(),
    )
    .unwrap();
    let authority = authority_for(
        payload.canonical_payload_digest().unwrap(),
        payload.egress_authorization_digest().unwrap(),
        payload.effect_digest().unwrap(),
    );
    MutationRequestEnvelope::new(MutationRequestEnvelopeParts::new(
        payload,
        authority.0,
        authority.1,
        authority.2,
    ))
    .unwrap()
}

#[test]
fn serialized_envelope_has_no_public_executable_construction_surface() {
    let source = include_str!("envelope.rs");
    let fields = source
        .split_once("pub struct MutationRequestEnvelope {")
        .unwrap()
        .1
        .split_once("\n}")
        .unwrap()
        .0;
    assert!(!fields.contains("    pub "));
    assert!(!fields.contains("    pub(crate)"));
    assert!(source.contains("Untrusted serialized mutation request and evidence"));
    assert!(source.contains("crate-private, non-`Deserialize`, non-`Clone` admitted plan/token"));
    let forbidden_execution_claim = ["accepted by the final mutation", " kernel"].concat();
    assert!(!source.contains(&forbidden_execution_claim));
}

#[test]
fn checked_constructor_accessors_and_named_round_trip_are_complete() {
    let envelope = envelope_with_effect_count(1);
    assert_eq!(envelope.mutation_id().as_str(), "mutation:1");
    assert_eq!(envelope.effects().len(), 1);
    assert_eq!(envelope.outbox().len(), 1);
    assert_eq!(
        envelope.operation_identity().canonical_payload_digest,
        envelope.canonical_payload_digest()
    );
    assert_eq!(
        envelope.nonce_replay_key().nonce,
        envelope.verified_authority().context.nonce
    );

    let named = rmp_serde::to_vec_named(&envelope).unwrap();
    let decoded = MutationRequestEnvelope::decode_msgpack(&named).unwrap();
    assert_eq!(decoded, envelope);

    let positional = rmp_serde::to_vec(&envelope).unwrap();
    assert!(MutationRequestEnvelope::decode_msgpack(&positional).is_err());
}

#[test]
fn envelope_rejects_unknown_and_duplicate_map_fields() {
    #[derive(Serialize)]
    struct UnknownEnvelopeField {
        unexpected: u8,
    }

    struct DuplicateEnvelopeField<'a>(&'a ResourceId);

    impl Serialize for DuplicateEnvelopeField<'_> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let mut map = serializer.serialize_map(Some(2))?;
            map.serialize_entry("schema_version", self.0)?;
            map.serialize_entry("schema_version", self.0)?;
            map.end()
        }
    }

    let unknown = rmp_serde::to_vec_named(&UnknownEnvelopeField { unexpected: 1 }).unwrap();
    assert!(rmp_serde::from_slice::<MutationRequestEnvelope>(&unknown).is_err());
    let schema = ResourceId::new(MUTATION_ENVELOPE_SCHEMA_V1).unwrap();
    let duplicate = rmp_serde::to_vec_named(&DuplicateEnvelopeField(&schema)).unwrap();
    assert!(rmp_serde::from_slice::<MutationRequestEnvelope>(&duplicate).is_err());
}

#[test]
fn four_thousand_ninety_six_small_effects_round_trip_under_budget() {
    let envelope = envelope_with_effect_count(MAX_MUTATION_EFFECTS);
    let named = rmp_serde::to_vec_named(&envelope).unwrap();
    let decoded = MutationRequestEnvelope::decode_msgpack(&named).unwrap();
    assert_eq!(decoded.effects().len(), MAX_MUTATION_EFFECTS);
}

#[test]
fn checked_payload_rejects_aggregate_over_sixteen_mib() {
    let effects = (0..16_u32)
        .map(|ordinal| {
            let payload =
                RecordBytes::new(vec![ordinal as u8; crate::contract::MAX_RECORD_BYTES]).unwrap();
            MutationEffect {
                ordinal,
                target: RecordTarget {
                    tenant: TenantId::new("tenant:a").unwrap(),
                    scope_digest: scope().digest().unwrap(),
                    domain: MutationDomain::new("graph").unwrap(),
                    schema_id: SchemaId::new("node.v1").unwrap(),
                    record_id: ResourceId::new(format!("node:{ordinal}")).unwrap(),
                },
                mutation: RecordMutation::Put {
                    payload_digest: payload.digest().unwrap(),
                    payload,
                },
            }
        })
        .collect();
    let result = MutationPayload::new(
        OpaqueId::new("mutation:large").unwrap(),
        scope(),
        BoundedVec::new(vec![]).unwrap(),
        BoundedVec::new(effects).unwrap(),
        BoundedVec::new(vec![]).unwrap(),
        BoundedVec::new(vec![]).unwrap(),
        RequestedMutationResult::new("receipt_only").unwrap(),
    );
    assert!(result.is_err());
}

#[test]
fn egress_destination_must_match_the_authority_decision() {
    let payload = MutationPayload::new(
        OpaqueId::new("mutation:egress").unwrap(),
        scope(),
        BoundedVec::new(vec![]).unwrap(),
        BoundedVec::new(vec![delete_effect(0)]).unwrap(),
        BoundedVec::new(vec![intent("topic:actual")]).unwrap(),
        BoundedVec::new(vec![]).unwrap(),
        RequestedMutationResult::new("receipt_only").unwrap(),
    )
    .unwrap();
    let authorized = BoundedVec::new(vec![intent("topic:authorized")]).unwrap();
    let authority = authority_for(
        payload.canonical_payload_digest().unwrap(),
        recompute_egress_authorization_digest(&authorized).unwrap(),
        payload.effect_digest().unwrap(),
    );
    assert!(
        MutationRequestEnvelope::new(MutationRequestEnvelopeParts::new(
            payload,
            authority.0,
            authority.1,
            authority.2,
        ))
        .is_err()
    );
}

#[test]
fn record_payload_is_bounded_before_envelope_admission() {
    assert!(RecordBytes::new(vec![0; crate::contract::MAX_RECORD_BYTES]).is_ok());
    assert!(RecordBytes::new(vec![0; crate::contract::MAX_RECORD_BYTES + 1]).is_err());
}

#[test]
fn effect_digest_binds_target_and_operation() {
    let target = RecordTarget {
        tenant: TenantId::new("tenant:a").unwrap(),
        scope_digest: scope().digest().unwrap(),
        domain: MutationDomain::new("graph").unwrap(),
        schema_id: SchemaId::new("node.v1").unwrap(),
        record_id: ResourceId::new("node:1").unwrap(),
    };
    let payload = RecordBytes::new(vec![1]).unwrap();
    let put = MutationEffect {
        ordinal: 0,
        target: target.clone(),
        mutation: RecordMutation::Put {
            payload_digest: payload.digest().unwrap(),
            payload,
        },
    };
    let delete = MutationEffect {
        ordinal: 0,
        target,
        mutation: RecordMutation::Delete {
            expected_record_digest: Digest256::from_bytes([1; 32]),
        },
    };
    assert_ne!(put.digest().unwrap(), delete.digest().unwrap());
}

#[test]
fn targets_reject_tenant_or_scope_substitution() {
    let authorized_scope = scope();
    let mut target = RecordTarget {
        tenant: TenantId::new("tenant:a").unwrap(),
        scope_digest: authorized_scope.digest().unwrap(),
        domain: MutationDomain::new("graph").unwrap(),
        schema_id: SchemaId::new("node.v1").unwrap(),
        record_id: ResourceId::new("node:1").unwrap(),
    };
    assert!(target
        .validate_for_scope(&TenantId::new("tenant:a").unwrap(), &authorized_scope)
        .is_ok());
    target.scope_digest = Digest256::from_bytes([99; 32]);
    assert!(target
        .validate_for_scope(&TenantId::new("tenant:a").unwrap(), &authorized_scope)
        .is_err());
}

#[test]
fn budgeted_nested_payload_rejects_before_copy_or_push() {
    let target = RecordTarget {
        tenant: TenantId::new("tenant:a").unwrap(),
        scope_digest: scope().digest().unwrap(),
        domain: MutationDomain::new("graph").unwrap(),
        schema_id: SchemaId::new("node.v1").unwrap(),
        record_id: ResourceId::new("node:1").unwrap(),
    };
    let payload = RecordBytes::new(vec![1]).unwrap();
    let encoded = rmp_serde::to_vec_named(&vec![MutationEffect {
        ordinal: 0,
        target,
        mutation: RecordMutation::Put {
            payload_digest: payload.digest().unwrap(),
            payload,
        },
    }])
    .unwrap();
    let mut budget = StructuralBudget {
        used: MAX_MUTATION_ENVELOPE_BYTES,
    };
    let mut deserializer = rmp_serde::Deserializer::new(encoded.as_slice());
    let decoded = BudgetedVecSeed::<MutationEffect, MAX_MUTATION_EFFECTS>::new(&mut budget)
        .deserialize(&mut deserializer);
    assert!(decoded.is_err());
    assert_eq!(budget.used, MAX_MUTATION_ENVELOPE_BYTES);
}

#[test]
fn budgeted_outbox_rejects_before_headers_decode_or_payload_copy() {
    let mut with_header = intent("topic:a");
    with_header.headers = BoundedVec::new(vec![OutboxHeader {
        name: ResourceId::new("header:a").unwrap(),
        value: OpaqueId::new("value:a").unwrap(),
    }])
    .unwrap();
    let encoded = rmp_serde::to_vec_named(&vec![with_header]).unwrap();
    let mut budget = StructuralBudget {
        used: MAX_MUTATION_ENVELOPE_BYTES - 3,
    };
    let mut deserializer = rmp_serde::Deserializer::new(encoded.as_slice());
    let decoded = BudgetedVecSeed::<OutboxIntent, MAX_OUTBOX_INTENTS>::new(&mut budget)
        .deserialize(&mut deserializer);
    assert!(decoded.is_err());
    assert_eq!(budget.used, MAX_MUTATION_ENVELOPE_BYTES - 3);
}

#[test]
fn record_target_messagepack_rejects_unknown_fields() {
    #[derive(Serialize)]
    struct HostileRecordTarget {
        tenant: TenantId,
        scope_digest: Digest256,
        domain: MutationDomain,
        schema_id: SchemaId,
        record_id: ResourceId,
        unexpected: u8,
    }

    let encoded = rmp_serde::to_vec_named(&HostileRecordTarget {
        tenant: TenantId::new("tenant:a").unwrap(),
        scope_digest: scope().digest().unwrap(),
        domain: MutationDomain::new("graph").unwrap(),
        schema_id: SchemaId::new("node.v1").unwrap(),
        record_id: ResourceId::new("node:1").unwrap(),
        unexpected: 1,
    })
    .unwrap();
    assert!(rmp_serde::from_slice::<RecordTarget>(&encoded).is_err());
}

#[test]
fn mutation_receipt_rejects_result_tampering_and_missing_commit_binding() {
    let result = MutationResult::ReceiptOnly;
    let mut receipt = MutationReceipt {
        receipt_id: OpaqueId::new("receipt:1").unwrap(),
        mutation_id: OpaqueId::new("mutation:1").unwrap(),
        scope: scope(),
        authority_receipt_id: OpaqueId::new("authority:1").unwrap(),
        authority_evidence_digest: Digest256::from_bytes([6; 32]),
        disposition: MutationDisposition::new("committed").unwrap(),
        operation_replay_digest: Digest256::from_bytes([1; 32]),
        nonce_replay_digest: Digest256::from_bytes([2; 32]),
        envelope_digest: Digest256::from_bytes([3; 32]),
        effect_id: None,
        effect_digest: None,
        result_digest: result.digest().unwrap(),
        commit_id: None,
        result,
        recorded_at: UtcUnixNanos::new(10),
    };
    assert!(receipt.validate().is_err());
    receipt.effect_id = Some(OpaqueId::new("effect:1").unwrap());
    receipt.effect_digest = Some(Digest256::from_bytes([4; 32]));
    receipt.commit_id = Some(OpaqueId::new("commit:1").unwrap());
    receipt.result_digest = Digest256::from_bytes([5; 32]);
    assert!(receipt.validate().is_err());

    receipt.disposition = MutationDisposition::new("conflict").unwrap();
    receipt.effect_id = None;
    receipt.effect_digest = None;
    receipt.commit_id = None;
    receipt.result = MutationResult::ReceiptOnly;
    receipt.result_digest = receipt.result.digest().unwrap();
    assert!(receipt.validate().is_err());
    receipt.result = MutationResult::NoEffect {
        reason: ResourceId::new("replay:conflict").unwrap(),
    };
    receipt.result_digest = receipt.result.digest().unwrap();
    assert!(receipt.validate().is_ok());
}

#[test]
fn requested_result_matrix_is_exact() {
    let result = MutationResult::ReceiptOnly;
    assert!(result_matches_request(
        &RequestedMutationResult::new("receipt_only").unwrap(),
        &result,
    ));
    assert!(!result_matches_request(
        &RequestedMutationResult::new("domain_result").unwrap(),
        &result,
    ));
}

#[test]
fn receipt_disposition_is_closed_over_replay_lifecycle() {
    let envelope = envelope_with_effect_count(1);
    let result = MutationResult::ReceiptOnly;
    let mut receipt = MutationReceipt {
        receipt_id: OpaqueId::new("receipt:1").unwrap(),
        mutation_id: envelope.mutation_id().clone(),
        scope: envelope.scope().clone(),
        authority_receipt_id: envelope.authority_receipt_id().clone(),
        authority_evidence_digest: envelope.authority_evidence_digest(),
        disposition: MutationDisposition::new("committed").unwrap(),
        operation_replay_digest: envelope.operation_replay_digest(),
        nonce_replay_digest: envelope.nonce_replay_digest(),
        envelope_digest: envelope.envelope_digest(),
        effect_id: Some(OpaqueId::new("effect:1").unwrap()),
        effect_digest: Some(envelope.recompute_effect_digest().unwrap()),
        result_digest: result.digest().unwrap(),
        commit_id: Some(OpaqueId::new("commit:1").unwrap()),
        result,
        recorded_at: UtcUnixNanos::new(122),
    };
    assert!(receipt.validate_against(&envelope).is_ok());

    receipt.effect_id = Some(OpaqueId::new("effect:substituted").unwrap());
    assert!(receipt.validate_against(&envelope).is_err());
    receipt.effect_id = Some(OpaqueId::new("effect:1").unwrap());
    receipt.commit_id = Some(OpaqueId::new("commit:substituted").unwrap());
    assert!(receipt.validate_against(&envelope).is_err());
    receipt.commit_id = Some(OpaqueId::new("commit:1").unwrap());
    receipt.effect_digest = Some(digest(99));
    assert!(receipt.validate_against(&envelope).is_err());
    receipt.effect_digest = Some(envelope.recompute_effect_digest().unwrap());

    receipt.disposition = MutationDisposition::new("replayed").unwrap();
    assert!(receipt.validate_against(&envelope).is_err());

    let mut replayed_envelope = envelope.clone();
    replayed_envelope.verified_authority.replay_receipt.status =
        ReplayStatus::new("duplicate").unwrap();
    replayed_envelope
        .verified_authority
        .replay_receipt
        .prior_replay_receipt_id = Some(OpaqueId::new("replay:prior").unwrap());
    replayed_envelope
        .verified_authority
        .replay_receipt
        .prior_result_digest = replayed_envelope
        .verified_authority
        .replay_receipt
        .result_digest;
    replayed_envelope
        .verified_authority
        .replay_receipt
        .prior_effect_id = replayed_envelope
        .verified_authority
        .replay_receipt
        .effect_id
        .take();
    replayed_envelope
        .verified_authority
        .replay_receipt
        .prior_commit_id = replayed_envelope
        .verified_authority
        .replay_receipt
        .commit_id
        .take();
    replayed_envelope
        .verified_authority
        .replay_receipt
        .prior_effect_digest = replayed_envelope
        .verified_authority
        .replay_receipt
        .effect_digest
        .take();
    replayed_envelope.verified_authority.evidence_digest = replayed_envelope
        .verified_authority
        .recompute_evidence_digest()
        .unwrap();
    replayed_envelope.envelope_digest = replayed_envelope.recompute_envelope_digest().unwrap();
    receipt.authority_evidence_digest = replayed_envelope.authority_evidence_digest();
    receipt.envelope_digest = replayed_envelope.envelope_digest();
    assert!(receipt.validate_against(&replayed_envelope).is_ok());

    let mut substituted_prior = replayed_envelope.clone();
    substituted_prior
        .verified_authority
        .replay_receipt
        .prior_commit_id = Some(OpaqueId::new("commit:other").unwrap());
    substituted_prior.verified_authority.evidence_digest = substituted_prior
        .verified_authority
        .recompute_evidence_digest()
        .unwrap();
    substituted_prior.envelope_digest = substituted_prior.recompute_envelope_digest().unwrap();
    receipt.authority_evidence_digest = substituted_prior.authority_evidence_digest();
    receipt.envelope_digest = substituted_prior.envelope_digest();
    assert!(receipt.validate_against(&substituted_prior).is_err());

    let mut substituted_prior = replayed_envelope.clone();
    substituted_prior
        .verified_authority
        .replay_receipt
        .prior_effect_id = Some(OpaqueId::new("effect:other").unwrap());
    substituted_prior.verified_authority.evidence_digest = substituted_prior
        .verified_authority
        .recompute_evidence_digest()
        .unwrap();
    substituted_prior.envelope_digest = substituted_prior.recompute_envelope_digest().unwrap();
    receipt.authority_evidence_digest = substituted_prior.authority_evidence_digest();
    receipt.envelope_digest = substituted_prior.envelope_digest();
    assert!(receipt.validate_against(&substituted_prior).is_err());

    let mut substituted_prior = replayed_envelope.clone();
    substituted_prior
        .verified_authority
        .replay_receipt
        .prior_effect_digest = Some(digest(99));
    substituted_prior.verified_authority.evidence_digest = substituted_prior
        .verified_authority
        .recompute_evidence_digest()
        .unwrap();
    substituted_prior.envelope_digest = substituted_prior.recompute_envelope_digest().unwrap();
    receipt.authority_evidence_digest = substituted_prior.authority_evidence_digest();
    receipt.envelope_digest = substituted_prior.envelope_digest();
    assert!(receipt.validate_against(&substituted_prior).is_err());

    for (effect_state, retryable, disposition) in [
        ("failed_terminal", false, "conflict"),
        ("failed_retryable", true, "unavailable"),
    ] {
        let mut failed_envelope = envelope.clone();
        failed_envelope
            .verified_authority
            .replay_receipt
            .effect_state = EffectState::new(effect_state).unwrap();
        failed_envelope
            .verified_authority
            .replay_receipt
            .result_digest = None;
        failed_envelope.verified_authority.replay_receipt.commit_id = None;
        failed_envelope
            .verified_authority
            .replay_receipt
            .effect_digest = None;
        failed_envelope.verified_authority.replay_receipt.retryable = retryable;
        failed_envelope.verified_authority.evidence_digest = failed_envelope
            .verified_authority
            .recompute_evidence_digest()
            .unwrap();
        failed_envelope.envelope_digest = failed_envelope.recompute_envelope_digest().unwrap();
        let result = MutationResult::NoEffect {
            reason: ResourceId::new("replay:failed").unwrap(),
        };
        let failed_receipt = MutationReceipt {
            receipt_id: OpaqueId::new(format!("receipt:{disposition}")).unwrap(),
            mutation_id: failed_envelope.mutation_id().clone(),
            scope: failed_envelope.scope().clone(),
            authority_receipt_id: failed_envelope.authority_receipt_id().clone(),
            authority_evidence_digest: failed_envelope.authority_evidence_digest(),
            disposition: MutationDisposition::new(disposition).unwrap(),
            operation_replay_digest: failed_envelope.operation_replay_digest(),
            nonce_replay_digest: failed_envelope.nonce_replay_digest(),
            envelope_digest: failed_envelope.envelope_digest(),
            effect_id: None,
            effect_digest: None,
            result_digest: result.digest().unwrap(),
            commit_id: None,
            result,
            recorded_at: UtcUnixNanos::new(122),
        };
        assert!(failed_receipt.validate_against(&failed_envelope).is_ok());
    }
}
