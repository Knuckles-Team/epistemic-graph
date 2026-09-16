//! Regression coverage for typed SQL-owner compilation before envelope issuance.

use super::*;
use eg_types::change_envelope::CursorPosition;
use eg_types::contract::{BoundedVec, Digest256, Nonce, RecordBytes, ResourceId};
use eg_types::storage_wire::{
    SqlSourceBatch, SqlSourceBatchRequest, SqlSourceCell, SqlSourceDescriptor, SqlSourceJson,
    SqlSourceMappingDescriptor, SqlSourceText,
};

const ROW_MARKER: &str = "row-content-private-marker";
const MAPPING_MARKER: &[u8] = b"mapping-content-private-marker";

fn id(value: &str) -> ResourceId {
    ResourceId::new(value).unwrap()
}

fn request(value: &str) -> SqlSourceBatchRequest {
    SqlSourceBatchRequest::new(SqlSourceBatch {
        source: id("source-private-marker"),
        partition: SqlSourceText::new("partition-private-marker".into()).unwrap(),
        position: CursorPosition::Sequence(1),
        expected_previous: None,
        source_descriptor: SqlSourceDescriptor {
            provider: id("provider-private-marker"),
            dataset: id("dataset-private-marker"),
            metadata: SqlSourceJson::new(
                serde_json::json!({"description":"descriptor-private-marker"}),
            )
            .unwrap(),
        },
        mapping_descriptor: SqlSourceMappingDescriptor {
            format: id("json"),
            content: RecordBytes::new(MAPPING_MARKER.to_vec()).unwrap(),
        },
        table: id("table-private-marker"),
        columns: BoundedVec::new(vec![id("column-private-marker")]).unwrap(),
        rows: BoundedVec::new(vec![BoundedVec::new(vec![SqlSourceCell::Text(
            SqlSourceText::new(value.into()).unwrap(),
        )])
        .unwrap()])
        .unwrap(),
        expected_schema_version: 0,
        expected_schema_digest: Digest256::from_bytes([7; 32]),
    })
    .unwrap()
}

fn context(nonce: u8) -> CompileBatch<'static> {
    CompileBatch {
        batch_id: "sql-source-test",
        request_id: u64::from(nonce),
        attempt_nonce: Some(Nonce::from_bytes([nonce; 32])),
        principal: Some("agent:source-compiler-test"),
        tenant: "tenant-a",
        graph: "source-a",
        placement_epoch: 0,
        idempotency_key: "source-compiler-idempotency",
        expected_graph_version: Some(0),
        fencing_token: None,
        created_at_ms: u64::from(nonce),
        default_surface: MutationSurface::Query,
        authoritative_state: None,
    }
}

#[test]
fn typed_method_is_retained_and_bound_before_native_owner_admission() {
    let request = request(ROW_MARKER);
    let compiled = compile_sql_source_batch(context(1), request.clone()).unwrap();
    assert_eq!(
        rmp_serde::to_vec_named(&compiled.operations).unwrap(),
        rmp_serde::to_vec_named(&vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Query,
            domain: DurabilityDomain::SqlCatalog,
            method: Method::SqlSourceBatch { batch: request },
        }])
        .unwrap()
    );
    assert!(matches!(
        &compiled.operations[0].method,
        Method::SqlSourceBatch { .. }
    ));
    assert!(matches!(
        compiled.version_expectation,
        VersionExpectation::Native(0)
    ));
    assert!(compiled.authoritative_state.is_none());
    compiled.validate().unwrap();
    assert!(compiled.envelope.operation().is_some());
    let mut tampered = compiled.clone();
    tampered.operations[0].method = Method::SqlSourceBatch {
        batch: self::request("altered raw row"),
    };
    assert!(tampered.validate().is_err());
}

#[test]
fn fresh_nonce_occ_and_route_changes_preserve_business_identity() {
    let first = compile_sql_source_batch(context(1), request(ROW_MARKER)).unwrap();
    let mut retry_context = context(2);
    retry_context.expected_graph_version = Some(9);
    retry_context.placement_epoch = 8;
    retry_context.fencing_token = Some(8);
    let retry = compile_sql_source_batch(retry_context, request(ROW_MARKER)).unwrap();
    let first_envelope = first.envelope.operation().unwrap();
    let retry_envelope = retry.envelope.operation().unwrap();
    assert_ne!(
        first_envelope.authority.nonce,
        retry_envelope.authority.nonce
    );
    assert_ne!(
        first_envelope.nonce_replay_key().unwrap(),
        retry_envelope.nonce_replay_key().unwrap()
    );
    assert_eq!(
        first_envelope.operation_identity().unwrap(),
        retry_envelope.operation_identity().unwrap()
    );
    assert_eq!(
        first_envelope.canonical_payload_digest,
        retry_envelope.canonical_payload_digest
    );
    assert_eq!(first.outbox, retry.outbox);
}

#[test]
fn altered_content_changes_bound_identity_under_same_idempotency_key() {
    let first = compile_sql_source_batch(context(1), request(ROW_MARKER)).unwrap();
    let altered = compile_sql_source_batch(context(2), request("altered row content")).unwrap();
    let first = first.envelope.operation().unwrap();
    let altered = altered.envelope.operation().unwrap();
    assert_eq!(
        first.authority.idempotency_key,
        altered.authority.idempotency_key
    );
    assert_ne!(
        first.canonical_payload_digest,
        altered.canonical_payload_digest
    );
    assert_ne!(
        first.operation_identity().unwrap(),
        altered.operation_identity().unwrap()
    );
}

#[test]
fn outbox_contains_only_bound_digests_and_generic_invalidation() {
    let request = request(ROW_MARKER);
    let digests = request.canonical_digests().unwrap();
    let compiled = compile_sql_source_batch(context(1), request).unwrap();
    assert_eq!(compiled.outbox.len(), 2);
    let dirty = compiled
        .outbox
        .iter()
        .find(|intent| intent.topic == eg_types::semantic_index::SEMANTIC_SOURCE_DIRTY_TOPIC)
        .unwrap();
    let intent =
        eg_types::semantic_index::SemanticSourceDirtyIntent::from_canonical_cbor(&dirty.payload)
            .unwrap();
    assert_eq!(
        intent.input_digest,
        eg_types::semantic_index::SemanticDigest::from_bytes(*digests.batch_digest.as_bytes())
    );
    assert_eq!(
        intent.source_scope_digest,
        eg_types::semantic_index::SemanticDigest::from_bytes(
            *compiled.identity.binding_digest().as_bytes()
        )
    );
    for notification in &compiled.outbox {
        let encoded = rmp_serde::to_vec_named(notification).unwrap();
        for marker in [
            ROW_MARKER.as_bytes(),
            MAPPING_MARKER,
            b"descriptor-private-marker",
            b"source-private-marker",
            b"table-private-marker",
        ] {
            assert!(!encoded.windows(marker.len()).any(|window| window == marker));
        }
    }
    validate_reasoning_wakeup(&compiled);
}

#[cfg(feature = "epistemic-tms")]
fn validate_reasoning_wakeup(compiled: &MutationBatch) {
    let projection = compiled
        .outbox
        .iter()
        .find(|intent| intent.topic == "engine.projection.rebuild")
        .unwrap();
    let wakeup: eg_epistemic::ReasoningProjectionWakeup =
        rmp_serde::from_slice(&projection.payload).unwrap();
    assert_eq!(
        wakeup.events,
        vec![eg_epistemic::IncrementalReasoningEvent::InvalidateAll]
    );
    assert_eq!(wakeup.operation_count, 1);
}

#[cfg(not(feature = "epistemic-tms"))]
fn validate_reasoning_wakeup(_compiled: &MutationBatch) {}

#[test]
fn authoritative_graph_state_is_refused_before_reserved_issuer_use() {
    let mut context = context(1);
    context.authoritative_state = Some(MutationStateDescriptor {
        algorithm: "sha256".into(),
        digest: "00".repeat(32),
        source_graph_version: 0,
        target_graph_version: 1,
    });
    let error = compile_sql_source_batch(context, request(ROW_MARKER)).unwrap_err();
    assert_eq!(
        error,
        "SQL source append cannot use authoritative graph state"
    );
}

#[test]
fn generic_opaque_compiler_keeps_original_applymutation_bytes() {
    let method = Method::Sql {
        query: "INSERT INTO old_table VALUES ('private old SQL')".into(),
        params_msgpack: Vec::new(),
    };
    let actual = compile_opaque_method(
        context(1),
        &method,
        MutationSurface::Query,
        DurabilityDomain::SqlCatalog,
        "sql_catalog_operation",
    )
    .unwrap();
    let input_digest: [u8; 32] = Sha256::digest(rmp_serde::to_vec_named(&method).unwrap()).into();
    let expected_operation = MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Query,
        domain: DurabilityDomain::SqlCatalog,
        method: Method::ApplyMutation {
            event_type: "sql_catalog_operation".into(),
            query: format!("sha256:{}", hex::encode(input_digest)),
        },
    };
    let expected = finish_batch(
        context(1),
        vec![expected_operation],
        false,
        CompiledOutbox {
            extra: Vec::new(),
            semantic_source_dirty_input: Some(
                eg_types::semantic_index::SemanticDigest::from_bytes(input_digest),
            ),
            #[cfg(feature = "epistemic-tms")]
            reasoning_events: vec![eg_epistemic::IncrementalReasoningEvent::InvalidateAll],
        },
        None,
    )
    .unwrap();
    assert_eq!(
        rmp_serde::to_vec_named(&actual).unwrap(),
        rmp_serde::to_vec_named(&expected).unwrap()
    );
}
