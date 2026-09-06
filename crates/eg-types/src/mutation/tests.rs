use super::budget::{BudgetedVecSeed, StructuralBudget};
use super::payload::recompute_egress_authorization_digest;
use super::receipt::result_matches_request;
use super::*;
use serde::de::DeserializeSeed;
use serde::ser::SerializeMap;
use serde::Serialize;

use crate::authority::{
    AdmissionStateV1, AuthorityContextV1, AuthorityScopeV1, DecisionOutcomeV1, EffectStateV1,
    IngressSurfaceV1, NonceReplayKeyV1, OperationReplayIdentityV1, OperationV1, ReplayReceiptV1,
    ReplayStatusV1, ScopeKindV1, SignedAuthorityEnvelopeEvidenceV1, VerificationStatusV1,
    VerifiedAuthorityV1, AUTHORITY_CONTEXT_SCHEMA_V1, AUTHORITY_PROTOCOL_V1,
};
use crate::contract::{
    ActorIdV1, AudienceIdV1, BoundedVecV1, Digest256V1, Ed25519SignatureV1, IdempotencyKeyV1,
    MethodIdV1, NonceV1, OpaqueIdV1, PolicyRevisionV1, ProtocolIdV1, RecordBytesV1, ResourceIdV1,
    SchemaIdV1, TenantIdV1, UtcUnixNanosV1, MAX_MUTATION_EFFECTS, MAX_MUTATION_ENVELOPE_BYTES,
    MAX_OUTBOX_INTENTS, MAX_SCOPE_COMPONENTS,
};
use crate::outbox::{OutboxHeaderV1, OutboxIntentV1};

fn digest(byte: u8) -> Digest256V1 {
    Digest256V1::from_bytes([byte; 32])
}

fn scope() -> AuthorityScopeV1 {
    AuthorityScopeV1 {
        kind: ScopeKindV1::new("graph").unwrap(),
        scope_id: ResourceIdV1::new("graph:tenant:a/g").unwrap(),
        tenant: Some(TenantIdV1::new("tenant:a").unwrap()),
        parent_scope_ids: BoundedVecV1::<ResourceIdV1, MAX_SCOPE_COMPONENTS>::new(vec![
            ResourceIdV1::new("tenant:a").unwrap(),
        ])
        .unwrap(),
        graph_incarnation: None,
    }
}

fn intent(topic: &str) -> OutboxIntentV1 {
    let payload = RecordBytesV1::new(vec![1, 2, 3]).unwrap();
    OutboxIntentV1 {
        tenant: TenantIdV1::new("tenant:a").unwrap(),
        destination_scope_digest: scope().digest().unwrap(),
        topic: ResourceIdV1::new(topic).unwrap(),
        partition_key: ResourceIdV1::new("partition:a").unwrap(),
        event_schema: ResourceIdV1::new("event.v1").unwrap(),
        payload_digest: payload.digest().unwrap(),
        payload,
        headers: BoundedVecV1::new(vec![]).unwrap(),
    }
}

fn delete_effect(ordinal: u32) -> MutationEffectV1 {
    MutationEffectV1 {
        ordinal,
        target: RecordTargetV1 {
            tenant: TenantIdV1::new("tenant:a").unwrap(),
            scope_digest: scope().digest().unwrap(),
            domain: MutationDomainV1::new("graph").unwrap(),
            schema_id: SchemaIdV1::new("node.v1").unwrap(),
            record_id: ResourceIdV1::new(format!("node:{ordinal}")).unwrap(),
        },
        mutation: RecordMutationV1::Delete {
            expected_record_digest: digest(30),
        },
    }
}

fn authority_for(
    canonical_payload_digest: Digest256V1,
    egress_authorization_digest: Digest256V1,
    effect_digest: Digest256V1,
) -> (
    VerifiedAuthorityV1,
    OperationReplayIdentityV1,
    NonceReplayKeyV1,
) {
    let mut context = AuthorityContextV1 {
        schema_version: ResourceIdV1::new(AUTHORITY_CONTEXT_SCHEMA_V1).unwrap(),
        protocol_id: ProtocolIdV1::new(AUTHORITY_PROTOCOL_V1).unwrap(),
        catalog_digest: digest(1),
        request_id: OpaqueIdV1::new("request:1").unwrap(),
        trace_id: OpaqueIdV1::new("trace:1").unwrap(),
        ingress_surface: IngressSurfaceV1::new("au_mcp").unwrap(),
        actor: ActorIdV1::new("actor:a").unwrap(),
        audience: AudienceIdV1::new("eg").unwrap(),
        tenant: TenantIdV1::new("tenant:a").unwrap(),
        authority_scope: scope(),
        purpose_kind: crate::authority::PurposeKindV1::new("graph_write").unwrap(),
        purpose_resource: Some(scope().scope_id),
        operation: OperationV1::new("mutation").unwrap(),
        policy_revision: PolicyRevisionV1::new("policy:1").unwrap(),
        policy_epoch: 7,
        policy_decision_id: OpaqueIdV1::new("decision:1").unwrap(),
        policy_digest: digest(2),
        issued_at: UtcUnixNanosV1::new(100),
        expires_at: UtcUnixNanosV1::new(10_100),
        nonce: NonceV1::from_bytes([3; 32]),
        idempotency_key: Some(IdempotencyKeyV1::new("idem:stable").unwrap()),
        context_digest: digest(0),
    };
    context.context_digest = context.recompute_context_digest().unwrap();
    let operation = OperationReplayIdentityV1::from_context(
        &context,
        MethodIdV1::new("mutation.apply").unwrap(),
        SchemaIdV1::new(MUTATION_ENVELOPE_SCHEMA_V1).unwrap(),
        digest(8),
        canonical_payload_digest,
    )
    .unwrap();
    let nonce = NonceReplayKeyV1::from_context(&context).unwrap();
    let mut signed_envelope = SignedAuthorityEnvelopeEvidenceV1 {
        protocol_id: context.protocol_id.clone(),
        schema_version: ResourceIdV1::new("signed-authority-envelope.v1").unwrap(),
        catalog_digest: context.catalog_digest,
        context_digest: context.context_digest,
        payload_digest: canonical_payload_digest,
        audience: context.audience.clone(),
        issued_at: context.issued_at,
        expires_at: context.expires_at,
        signer_key_id: OpaqueIdV1::new("signer:1").unwrap(),
        signer_key_version: 3,
        signature_algorithm: ResourceIdV1::new("ed25519").unwrap(),
        canonicalization: ResourceIdV1::new("canonical-msgpack.v1").unwrap(),
        unsigned_message_digest: digest(0),
        signature: Ed25519SignatureV1::from_bytes([20; 64]),
        envelope_digest: digest(0),
    };
    signed_envelope.unsigned_message_digest =
        signed_envelope.recompute_unsigned_message_digest().unwrap();
    signed_envelope.envelope_digest = signed_envelope.recompute_envelope_digest().unwrap();
    let operation_digest = operation.digest().unwrap();
    let nonce_digest = nonce.digest().unwrap();
    let result_digest = MutationResultV1::ReceiptOnly.digest().unwrap();
    let mut authority = VerifiedAuthorityV1 {
        receipt_id: OpaqueIdV1::new("authority:1").unwrap(),
        context: context.clone(),
        signed_envelope: signed_envelope.clone(),
        verification_receipt_id: OpaqueIdV1::new("verification:1").unwrap(),
        verification_status: VerificationStatusV1::new("verified").unwrap(),
        envelope_digest: signed_envelope.envelope_digest,
        verified_context_digest: context.context_digest,
        verified_payload_digest: canonical_payload_digest,
        verified_catalog_digest: context.catalog_digest,
        verified_method: operation.method.clone(),
        verified_method_schema_id: operation.method_schema_id.clone(),
        verified_method_schema_digest: operation.method_schema_digest,
        signer_key_id: signed_envelope.signer_key_id.clone(),
        verified_at: UtcUnixNanosV1::new(110),
        verification_valid_until: UtcUnixNanosV1::new(9_000),
        admission_receipt_id: OpaqueIdV1::new("admission:1").unwrap(),
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
        admission_state: AdmissionStateV1::new("admitted").unwrap(),
        admission_issued_at: UtcUnixNanosV1::new(115),
        admission_expires_at: UtcUnixNanosV1::new(8_000),
        replay_receipt: ReplayReceiptV1 {
            receipt_id: OpaqueIdV1::new("replay:1").unwrap(),
            context_digest: context.context_digest,
            operation_replay_digest: operation_digest,
            nonce_replay_digest: nonce_digest,
            replay_ledger_epoch: Some(9),
            nonce_consumed_at: Some(UtcUnixNanosV1::new(120)),
            recorded_at: UtcUnixNanosV1::new(121),
            status: ReplayStatusV1::new("consumed").unwrap(),
            effect_state: EffectStateV1::new("committed").unwrap(),
            effect_id: Some(OpaqueIdV1::new("effect:1").unwrap()),
            commit_id: Some(OpaqueIdV1::new("commit:1").unwrap()),
            effect_digest: Some(effect_digest),
            result_digest: Some(result_digest),
            prior_replay_receipt_id: None,
            prior_effect_id: None,
            prior_commit_id: None,
            prior_effect_digest: None,
            prior_result_digest: None,
            retryable: false,
        },
        valid_until: UtcUnixNanosV1::new(8_000),
        durable_identity_epoch: 4,
        decision_id: context.policy_decision_id.clone(),
        parent_decision_id: None,
        decision_outcome: DecisionOutcomeV1::new("allow").unwrap(),
        decision_context_digest: context.context_digest,
        decision_policy_digest: context.policy_digest,
        decision_egress_authorization_digest: egress_authorization_digest,
        decision_policy_epoch: context.policy_epoch,
        decision_reason_code: ResourceIdV1::new("policy:allow").unwrap(),
        decision_retryable: false,
        operation_replay_digest: operation_digest,
        nonce_replay_digest: nonce_digest,
        evidence_digest: digest(0),
    };
    authority.evidence_digest = authority.recompute_evidence_digest().unwrap();
    (authority, operation, nonce)
}

fn envelope_with_effect_count(effect_count: usize) -> MutationEnvelopeV1 {
    let effects = (0..effect_count)
        .map(|ordinal| delete_effect(u32::try_from(ordinal).unwrap()))
        .collect();
    let payload = MutationPayloadV1::new(
        OpaqueIdV1::new("mutation:1").unwrap(),
        scope(),
        BoundedVecV1::new(vec![]).unwrap(),
        BoundedVecV1::new(effects).unwrap(),
        BoundedVecV1::new(vec![intent("topic:a")]).unwrap(),
        BoundedVecV1::new(vec![]).unwrap(),
        RequestedMutationResultV1::new("receipt_only").unwrap(),
    )
    .unwrap();
    let authority = authority_for(
        payload.canonical_payload_digest().unwrap(),
        payload.egress_authorization_digest().unwrap(),
        payload.effect_digest().unwrap(),
    );
    MutationEnvelopeV1::new(MutationEnvelopePartsV1::new(
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
        .split_once("pub struct MutationEnvelopeV1 {")
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
    let decoded = MutationEnvelopeV1::decode_msgpack(&named).unwrap();
    assert_eq!(decoded, envelope);

    let positional = rmp_serde::to_vec(&envelope).unwrap();
    assert!(MutationEnvelopeV1::decode_msgpack(&positional).is_err());
}

#[test]
fn envelope_rejects_unknown_and_duplicate_map_fields() {
    #[derive(Serialize)]
    struct UnknownEnvelopeField {
        unexpected: u8,
    }

    struct DuplicateEnvelopeField<'a>(&'a ResourceIdV1);

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
    assert!(rmp_serde::from_slice::<MutationEnvelopeV1>(&unknown).is_err());
    let schema = ResourceIdV1::new(MUTATION_ENVELOPE_SCHEMA_V1).unwrap();
    let duplicate = rmp_serde::to_vec_named(&DuplicateEnvelopeField(&schema)).unwrap();
    assert!(rmp_serde::from_slice::<MutationEnvelopeV1>(&duplicate).is_err());
}

#[test]
fn four_thousand_ninety_six_small_effects_round_trip_under_budget() {
    let envelope = envelope_with_effect_count(MAX_MUTATION_EFFECTS);
    let named = rmp_serde::to_vec_named(&envelope).unwrap();
    let decoded = MutationEnvelopeV1::decode_msgpack(&named).unwrap();
    assert_eq!(decoded.effects().len(), MAX_MUTATION_EFFECTS);
}

#[test]
fn checked_payload_rejects_aggregate_over_sixteen_mib() {
    let effects = (0..16_u32)
        .map(|ordinal| {
            let payload =
                RecordBytesV1::new(vec![ordinal as u8; crate::contract::MAX_RECORD_BYTES]).unwrap();
            MutationEffectV1 {
                ordinal,
                target: RecordTargetV1 {
                    tenant: TenantIdV1::new("tenant:a").unwrap(),
                    scope_digest: scope().digest().unwrap(),
                    domain: MutationDomainV1::new("graph").unwrap(),
                    schema_id: SchemaIdV1::new("node.v1").unwrap(),
                    record_id: ResourceIdV1::new(format!("node:{ordinal}")).unwrap(),
                },
                mutation: RecordMutationV1::Put {
                    payload_digest: payload.digest().unwrap(),
                    payload,
                },
            }
        })
        .collect();
    let result = MutationPayloadV1::new(
        OpaqueIdV1::new("mutation:large").unwrap(),
        scope(),
        BoundedVecV1::new(vec![]).unwrap(),
        BoundedVecV1::new(effects).unwrap(),
        BoundedVecV1::new(vec![]).unwrap(),
        BoundedVecV1::new(vec![]).unwrap(),
        RequestedMutationResultV1::new("receipt_only").unwrap(),
    );
    assert!(result.is_err());
}

#[test]
fn egress_destination_must_match_the_authority_decision() {
    let payload = MutationPayloadV1::new(
        OpaqueIdV1::new("mutation:egress").unwrap(),
        scope(),
        BoundedVecV1::new(vec![]).unwrap(),
        BoundedVecV1::new(vec![delete_effect(0)]).unwrap(),
        BoundedVecV1::new(vec![intent("topic:actual")]).unwrap(),
        BoundedVecV1::new(vec![]).unwrap(),
        RequestedMutationResultV1::new("receipt_only").unwrap(),
    )
    .unwrap();
    let authorized = BoundedVecV1::new(vec![intent("topic:authorized")]).unwrap();
    let authority = authority_for(
        payload.canonical_payload_digest().unwrap(),
        recompute_egress_authorization_digest(&authorized).unwrap(),
        payload.effect_digest().unwrap(),
    );
    assert!(MutationEnvelopeV1::new(MutationEnvelopePartsV1::new(
        payload,
        authority.0,
        authority.1,
        authority.2,
    ))
    .is_err());
}

#[test]
fn record_payload_is_bounded_before_envelope_admission() {
    assert!(RecordBytesV1::new(vec![0; crate::contract::MAX_RECORD_BYTES]).is_ok());
    assert!(RecordBytesV1::new(vec![0; crate::contract::MAX_RECORD_BYTES + 1]).is_err());
}

#[test]
fn effect_digest_binds_target_and_operation() {
    let target = RecordTargetV1 {
        tenant: TenantIdV1::new("tenant:a").unwrap(),
        scope_digest: scope().digest().unwrap(),
        domain: MutationDomainV1::new("graph").unwrap(),
        schema_id: SchemaIdV1::new("node.v1").unwrap(),
        record_id: ResourceIdV1::new("node:1").unwrap(),
    };
    let payload = RecordBytesV1::new(vec![1]).unwrap();
    let put = MutationEffectV1 {
        ordinal: 0,
        target: target.clone(),
        mutation: RecordMutationV1::Put {
            payload_digest: payload.digest().unwrap(),
            payload,
        },
    };
    let delete = MutationEffectV1 {
        ordinal: 0,
        target,
        mutation: RecordMutationV1::Delete {
            expected_record_digest: Digest256V1::from_bytes([1; 32]),
        },
    };
    assert_ne!(put.digest().unwrap(), delete.digest().unwrap());
}

#[test]
fn targets_reject_tenant_or_scope_substitution() {
    let authorized_scope = scope();
    let mut target = RecordTargetV1 {
        tenant: TenantIdV1::new("tenant:a").unwrap(),
        scope_digest: authorized_scope.digest().unwrap(),
        domain: MutationDomainV1::new("graph").unwrap(),
        schema_id: SchemaIdV1::new("node.v1").unwrap(),
        record_id: ResourceIdV1::new("node:1").unwrap(),
    };
    assert!(target
        .validate_for_scope(&TenantIdV1::new("tenant:a").unwrap(), &authorized_scope)
        .is_ok());
    target.scope_digest = Digest256V1::from_bytes([99; 32]);
    assert!(target
        .validate_for_scope(&TenantIdV1::new("tenant:a").unwrap(), &authorized_scope)
        .is_err());
}

#[test]
fn budgeted_nested_payload_rejects_before_copy_or_push() {
    let target = RecordTargetV1 {
        tenant: TenantIdV1::new("tenant:a").unwrap(),
        scope_digest: scope().digest().unwrap(),
        domain: MutationDomainV1::new("graph").unwrap(),
        schema_id: SchemaIdV1::new("node.v1").unwrap(),
        record_id: ResourceIdV1::new("node:1").unwrap(),
    };
    let payload = RecordBytesV1::new(vec![1]).unwrap();
    let encoded = rmp_serde::to_vec_named(&vec![MutationEffectV1 {
        ordinal: 0,
        target,
        mutation: RecordMutationV1::Put {
            payload_digest: payload.digest().unwrap(),
            payload,
        },
    }])
    .unwrap();
    let mut budget = StructuralBudget {
        used: MAX_MUTATION_ENVELOPE_BYTES,
    };
    let mut deserializer = rmp_serde::Deserializer::new(encoded.as_slice());
    let decoded = BudgetedVecSeed::<MutationEffectV1, MAX_MUTATION_EFFECTS>::new(&mut budget)
        .deserialize(&mut deserializer);
    assert!(decoded.is_err());
    assert_eq!(budget.used, MAX_MUTATION_ENVELOPE_BYTES);
}

#[test]
fn budgeted_outbox_rejects_before_headers_decode_or_payload_copy() {
    let mut with_header = intent("topic:a");
    with_header.headers = BoundedVecV1::new(vec![OutboxHeaderV1 {
        name: ResourceIdV1::new("header:a").unwrap(),
        value: OpaqueIdV1::new("value:a").unwrap(),
    }])
    .unwrap();
    let encoded = rmp_serde::to_vec_named(&vec![with_header]).unwrap();
    let mut budget = StructuralBudget {
        used: MAX_MUTATION_ENVELOPE_BYTES - 3,
    };
    let mut deserializer = rmp_serde::Deserializer::new(encoded.as_slice());
    let decoded = BudgetedVecSeed::<OutboxIntentV1, MAX_OUTBOX_INTENTS>::new(&mut budget)
        .deserialize(&mut deserializer);
    assert!(decoded.is_err());
    assert_eq!(budget.used, MAX_MUTATION_ENVELOPE_BYTES - 3);
}

#[test]
fn record_target_messagepack_rejects_unknown_fields() {
    #[derive(Serialize)]
    struct HostileRecordTarget {
        tenant: TenantIdV1,
        scope_digest: Digest256V1,
        domain: MutationDomainV1,
        schema_id: SchemaIdV1,
        record_id: ResourceIdV1,
        unexpected: u8,
    }

    let encoded = rmp_serde::to_vec_named(&HostileRecordTarget {
        tenant: TenantIdV1::new("tenant:a").unwrap(),
        scope_digest: scope().digest().unwrap(),
        domain: MutationDomainV1::new("graph").unwrap(),
        schema_id: SchemaIdV1::new("node.v1").unwrap(),
        record_id: ResourceIdV1::new("node:1").unwrap(),
        unexpected: 1,
    })
    .unwrap();
    assert!(rmp_serde::from_slice::<RecordTargetV1>(&encoded).is_err());
}

#[test]
fn mutation_receipt_rejects_result_tampering_and_missing_commit_binding() {
    let result = MutationResultV1::ReceiptOnly;
    let mut receipt = MutationReceiptV1 {
        receipt_id: OpaqueIdV1::new("receipt:1").unwrap(),
        mutation_id: OpaqueIdV1::new("mutation:1").unwrap(),
        scope: scope(),
        authority_receipt_id: OpaqueIdV1::new("authority:1").unwrap(),
        authority_evidence_digest: Digest256V1::from_bytes([6; 32]),
        disposition: MutationDispositionV1::new("committed").unwrap(),
        operation_replay_digest: Digest256V1::from_bytes([1; 32]),
        nonce_replay_digest: Digest256V1::from_bytes([2; 32]),
        envelope_digest: Digest256V1::from_bytes([3; 32]),
        effect_id: None,
        effect_digest: None,
        result_digest: result.digest().unwrap(),
        commit_id: None,
        result,
        recorded_at: UtcUnixNanosV1::new(10),
    };
    assert!(receipt.validate().is_err());
    receipt.effect_id = Some(OpaqueIdV1::new("effect:1").unwrap());
    receipt.effect_digest = Some(Digest256V1::from_bytes([4; 32]));
    receipt.commit_id = Some(OpaqueIdV1::new("commit:1").unwrap());
    receipt.result_digest = Digest256V1::from_bytes([5; 32]);
    assert!(receipt.validate().is_err());

    receipt.disposition = MutationDispositionV1::new("conflict").unwrap();
    receipt.effect_id = None;
    receipt.effect_digest = None;
    receipt.commit_id = None;
    receipt.result = MutationResultV1::ReceiptOnly;
    receipt.result_digest = receipt.result.digest().unwrap();
    assert!(receipt.validate().is_err());
    receipt.result = MutationResultV1::NoEffect {
        reason: ResourceIdV1::new("replay:conflict").unwrap(),
    };
    receipt.result_digest = receipt.result.digest().unwrap();
    assert!(receipt.validate().is_ok());
}

#[test]
fn requested_result_matrix_is_exact() {
    let result = MutationResultV1::ReceiptOnly;
    assert!(result_matches_request(
        &RequestedMutationResultV1::new("receipt_only").unwrap(),
        &result,
    ));
    assert!(!result_matches_request(
        &RequestedMutationResultV1::new("domain_result").unwrap(),
        &result,
    ));
}

#[test]
fn receipt_disposition_is_closed_over_replay_lifecycle() {
    let envelope = envelope_with_effect_count(1);
    let result = MutationResultV1::ReceiptOnly;
    let mut receipt = MutationReceiptV1 {
        receipt_id: OpaqueIdV1::new("receipt:1").unwrap(),
        mutation_id: envelope.mutation_id().clone(),
        scope: envelope.scope().clone(),
        authority_receipt_id: envelope.authority_receipt_id().clone(),
        authority_evidence_digest: envelope.authority_evidence_digest(),
        disposition: MutationDispositionV1::new("committed").unwrap(),
        operation_replay_digest: envelope.operation_replay_digest(),
        nonce_replay_digest: envelope.nonce_replay_digest(),
        envelope_digest: envelope.envelope_digest(),
        effect_id: Some(OpaqueIdV1::new("effect:1").unwrap()),
        effect_digest: Some(envelope.recompute_effect_digest().unwrap()),
        result_digest: result.digest().unwrap(),
        commit_id: Some(OpaqueIdV1::new("commit:1").unwrap()),
        result,
        recorded_at: UtcUnixNanosV1::new(122),
    };
    assert!(receipt.validate_against(&envelope).is_ok());

    receipt.effect_id = Some(OpaqueIdV1::new("effect:substituted").unwrap());
    assert!(receipt.validate_against(&envelope).is_err());
    receipt.effect_id = Some(OpaqueIdV1::new("effect:1").unwrap());
    receipt.commit_id = Some(OpaqueIdV1::new("commit:substituted").unwrap());
    assert!(receipt.validate_against(&envelope).is_err());
    receipt.commit_id = Some(OpaqueIdV1::new("commit:1").unwrap());
    receipt.effect_digest = Some(digest(99));
    assert!(receipt.validate_against(&envelope).is_err());
    receipt.effect_digest = Some(envelope.recompute_effect_digest().unwrap());

    receipt.disposition = MutationDispositionV1::new("replayed").unwrap();
    assert!(receipt.validate_against(&envelope).is_err());

    let mut replayed_envelope = envelope.clone();
    replayed_envelope.verified_authority.replay_receipt.status =
        ReplayStatusV1::new("duplicate").unwrap();
    replayed_envelope
        .verified_authority
        .replay_receipt
        .prior_replay_receipt_id = Some(OpaqueIdV1::new("replay:prior").unwrap());
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
        .prior_commit_id = Some(OpaqueIdV1::new("commit:other").unwrap());
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
        .prior_effect_id = Some(OpaqueIdV1::new("effect:other").unwrap());
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
            .effect_state = EffectStateV1::new(effect_state).unwrap();
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
        let result = MutationResultV1::NoEffect {
            reason: ResourceIdV1::new("replay:failed").unwrap(),
        };
        let failed_receipt = MutationReceiptV1 {
            receipt_id: OpaqueIdV1::new(format!("receipt:{disposition}")).unwrap(),
            mutation_id: failed_envelope.mutation_id().clone(),
            scope: failed_envelope.scope().clone(),
            authority_receipt_id: failed_envelope.authority_receipt_id().clone(),
            authority_evidence_digest: failed_envelope.authority_evidence_digest(),
            disposition: MutationDispositionV1::new(disposition).unwrap(),
            operation_replay_digest: failed_envelope.operation_replay_digest(),
            nonce_replay_digest: failed_envelope.nonce_replay_digest(),
            envelope_digest: failed_envelope.envelope_digest(),
            effect_id: None,
            effect_digest: None,
            result_digest: result.digest().unwrap(),
            commit_id: None,
            result,
            recorded_at: UtcUnixNanosV1::new(122),
        };
        assert!(failed_receipt.validate_against(&failed_envelope).is_ok());
    }
}
