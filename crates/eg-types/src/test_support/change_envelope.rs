//! One valid, validating `ChangeEnvelope`, shared by the envelope's own
//! strictness tests and the method-body vectors.

use crate::change_envelope::{
    ChangeEnvelope, ContentVersion, ContentVersionPosition, MaterialOperation, PolicyRecord,
    PrivacyAttestation, CHANGE_ENVELOPE_VERSION,
};

fn minimal_mutation_envelope() -> crate::mutation_batch::MutationEnvelope {
    let identity = crate::mutation_batch::MutationScopeIdentity::graph(
        crate::mutation_batch::ScopeTenantId::new("tenant-a").unwrap(),
        crate::mutation_batch::LogicalName::new("graph-a").unwrap(),
        crate::mutation_batch::IncarnationId::new("incarnation:test:change-envelope").unwrap(),
    );
    let method =
        crate::contract::MethodId::new(crate::mutation_batch::BATCH_COMPILED_METHODS).unwrap();
    crate::mutation_batch::MutationEnvelope::for_scope(
        crate::mutation_batch::CompiledScope {
            identity: &identity,
            actor: &format!("principal:sha256:{}", "a".repeat(64)),
            serving_principal: &format!("principal:sha256:{}", "a".repeat(64)),
            request_id: 1,
            idempotency_key: "idem-1",
            nonce: crate::contract::Nonce::from_bytes([1_u8; 32]),
            now_ms: 0,
        },
        crate::mutation_batch::CompiledOperation {
            method_schema_id: crate::mutation_batch::method_schema_id(&method).unwrap(),
            method,
            method_schema_digest: crate::contract::Digest256::from_bytes([1_u8; 32]),
            canonical_payload_digest: crate::contract::Digest256::from_bytes([2_u8; 32]),
        },
    )
    .unwrap()
}

pub fn minimal_envelope() -> ChangeEnvelope {
    let mut mutation = crate::mutation_batch::MutationBatch {
        schema_version: crate::mutation_batch::MUTATION_BATCH_VERSION,
        batch_id: "batch-1".into(),
        envelope: minimal_mutation_envelope(),
        identity: crate::mutation_batch::MutationScopeIdentity::graph(
            crate::mutation_batch::ScopeTenantId::new("tenant-a").unwrap(),
            crate::mutation_batch::LogicalName::new("graph-a").unwrap(),
            crate::mutation_batch::IncarnationId::new("incarnation:test:change-envelope").unwrap(),
        ),
        placement_epoch: 0,
        version_expectation: crate::mutation_batch::VersionExpectation::Graph(0),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![crate::mutation_batch::MutationOperation {
            ordinal: 0,
            surface: crate::mutation_batch::MutationSurface::Graph,
            domain: crate::mutation_batch::DurabilityDomain::GraphRows,
            method: crate::protocol::Method::AddNode {
                node_id: "n1".into(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"value": 1}))
                    .unwrap(),
            },
        }],
        outbox: Vec::new(),
        created_at_ms: 0,
    };
    mutation
        .reseal_envelope(crate::contract::Digest256::from_bytes([1_u8; 32]))
        .expect("a fixture batch reseals");
    ChangeEnvelope {
        schema_version: CHANGE_ENVELOPE_VERSION,
        envelope_id: "envelope-1".into(),
        mutation,
        content_version: ContentVersion {
            object_id: "object-1".into(),
            digest_algorithm: "sha256".into(),
            digest: "a".repeat(64),
            previous_digest: None,
            source_version: ContentVersionPosition::Sequence(1),
        },
        cursor: None,
        blobs: Vec::new(),
        features: Vec::new(),
        evidence: Vec::new(),
        // `validate()` ALWAYS seeds `required_governance` with
        // `content_version.object_id`, and `governed` is filled only from
        // `policies` — so an envelope with no policy record can never
        // validate, regardless of how minimal it is otherwise. An empty
        // `policies` here made this fixture unconstructible-as-valid and
        // every test calling `.validate()` on it failed with
        // "no policy proof for material object 'object-1'". "Minimal" must
        // mean minimal-and-valid, or it proves nothing about the field
        // under test.
        policies: vec![PolicyRecord {
            policy_id: "policy-1".into(),
            operation: MaterialOperation::Upsert,
            object_id: "object-1".into(),
            tenant: "tenant-a".into(),
            classification: "internal".into(),
            policy_version: "policy-v1".into(),
            subject_set_digest: "c".repeat(64),
            retention_policy: "default".into(),
            legal_hold: false,
        }],
        lineage: Vec::new(),
        privacy: PrivacyAttestation {
            policy_version: "privacy-v1".into(),
            sanitizer_version: "sanitizer-v1".into(),
            sanitized_payload_digest: "b".repeat(64),
        },
        commit_seq: None,
        commit_descriptor_ref: None,
    }
}
