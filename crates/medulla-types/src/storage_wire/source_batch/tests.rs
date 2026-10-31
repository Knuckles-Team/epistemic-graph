use super::*;
use serde_json::json;

use crate::test_support::sql_source::{batch as shared_batch, id, SqlSourceTarget};

/// The shared fixture at its second position, so cursor-advance checks apply.
fn batch(cells: Vec<SqlSourceCell>) -> SqlSourceBatch {
    let columns: Vec<String> = (0..cells.len()).map(|n| format!("col{n}")).collect();
    let columns: Vec<&str> = columns.iter().map(String::as_str).collect();
    let mut batch = shared_batch(
        &SqlSourceTarget {
            table: "issues",
            columns: &columns,
            schema_version: 0,
            schema_digest: Digest256::from_bytes([1; 32]),
        },
        vec![cells],
    );
    batch.expected_previous = Some(batch.position.clone());
    batch.position = CursorPosition::Sequence(2);
    batch
}

fn encoded(value: &impl Serialize) -> Vec<u8> {
    rmp_serde::to_vec_named(value).unwrap()
}

fn checked_decode(
    bytes: &[u8],
) -> Result<SqlSourceBatchRequest, crate::msgpack::MsgpackValidationError> {
    crate::msgpack::decode_bounded(
        bytes,
        crate::msgpack::MsgpackLimits::new(
            MAX_SQL_SOURCE_BATCH_BYTES,
            crate::msgpack::MAX_PROPERTY_ITEMS,
            crate::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
}

#[test]
fn sql_source_scalar_binary_json_and_vector_fidelity() {
    let bytes = vec![0, 10, 255, 42, 0];
    let request = SqlSourceBatchRequest::new(batch(vec![
        SqlSourceCell::Null,
        SqlSourceCell::Int(i64::MAX),
        SqlSourceCell::FiniteFloat(SqlSourceFloat::new(-0.0).unwrap()),
        SqlSourceCell::Text(SqlSourceText::new("東京\nissue".into()).unwrap()),
        SqlSourceCell::Bool(true),
        SqlSourceCell::Timestamp(i64::MIN),
        SqlSourceCell::Bytes(RecordBytes::new(bytes.clone()).unwrap()),
        SqlSourceCell::Json(SqlSourceJson::new(json!({"n": u64::MAX, "null": null})).unwrap()),
        SqlSourceCell::FiniteVector(
            SqlSourceVector::new(vec![f32::MAX, f32::MIN_POSITIVE]).unwrap(),
        ),
    ]))
    .unwrap();
    let wire = encoded(&request);
    let mut binary_prefix = vec![0xc4, bytes.len() as u8];
    binary_prefix.extend_from_slice(&bytes);
    assert!(wire
        .windows(binary_prefix.len())
        .any(|window| window == binary_prefix));
    let decoded = checked_decode(&wire).unwrap();
    assert_eq!(decoded, request);
    let SqlSourceCell::FiniteFloat(value) = decoded.as_batch().rows.as_slice()[0].as_slice()[2]
    else {
        panic!("expected finite float");
    };
    assert_eq!(value.get().to_bits(), (-0.0_f64).to_bits());
}

#[test]
fn sql_source_nonfinite_scalars_and_vectors_reject_during_decode() {
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(rmp_serde::from_slice::<SqlSourceFloat>(&encoded(&value)).is_err());
    }
    assert!(rmp_serde::from_slice::<SqlSourceVector>(&encoded(&vec![f32::NAN])).is_err());
    assert!(SqlSourceVector::new(Vec::new()).is_err());
    assert!(SqlSourceVector::new(vec![0.0; MAX_SQL_SOURCE_VECTOR_DIMENSIONS + 1]).is_err());
}

#[test]
fn sql_source_json_has_one_canonical_form_and_rejects_ambiguous_content() {
    let left = SqlSourceJson::from_bytes(
        RecordBytes::new(br#"{"b":{"z":2,"a":1},"a":0}"#.to_vec()).unwrap(),
    )
    .unwrap();
    let right = SqlSourceJson::from_bytes(
        RecordBytes::new(br#"{ "a":0, "b": {"a":1,"z":2} }"#.to_vec()).unwrap(),
    )
    .unwrap();
    assert_eq!(left.canonical_bytes(), right.canonical_bytes());
    assert!(
        SqlSourceJson::from_bytes(RecordBytes::new(br#"{"a":1,"a":2}"#.to_vec()).unwrap()).is_err()
    );
    assert!(SqlSourceJson::from_bytes(
        RecordBytes::new(br#"{"a":{"b":1,"b":2}}"#.to_vec()).unwrap()
    )
    .is_err());
    let mut deep = json!(0);
    for _ in 0..crate::msgpack::DEFAULT_MAX_DEPTH + 1 {
        deep = json!([deep]);
    }
    assert!(SqlSourceJson::new(deep).is_err());
}

#[test]
fn sql_source_semantic_digest_binds_rows_cursor_source_mapping_and_schema() {
    let original = batch(vec![SqlSourceCell::Int(1)]);
    let digest = SqlSourceBatchRequest::new(original.clone())
        .unwrap()
        .canonical_digests()
        .unwrap();
    let mut changes = Vec::new();
    let mut changed = original.clone();
    changed.source = id("servicenow");
    changes.push(changed);
    let mut changed = original.clone();
    changed.partition = SqlSourceText::new("other".into()).unwrap();
    changes.push(changed);
    let mut changed = original.clone();
    changed.position = CursorPosition::Sequence(3);
    changes.push(changed);
    let mut changed = original.clone();
    changed.rows =
        BoundedVec::new(vec![BoundedVec::new(vec![SqlSourceCell::Int(2)]).unwrap()]).unwrap();
    changes.push(changed);
    let mut changed = original.clone();
    changed.mapping_descriptor.content = RecordBytes::new(b"other mapping".to_vec()).unwrap();
    changes.push(changed);
    let mut changed = original.clone();
    changed.source_descriptor.dataset = id("other");
    changes.push(changed);
    let mut changed = original;
    changed.expected_schema_digest = Digest256::from_bytes([2; 32]);
    changes.push(changed);
    for changed in changes {
        assert_ne!(
            SqlSourceBatchRequest::new(changed)
                .unwrap()
                .canonical_digests()
                .unwrap()
                .batch_digest,
            digest.batch_digest
        );
    }
}

#[test]
fn sql_source_invalid_shape_and_cursor_reject_before_transaction() {
    let original = batch(vec![SqlSourceCell::Int(1)]);
    let mut unknown = serde_json::to_value(&original).unwrap();
    unknown["request_id"] = json!(42);
    assert!(checked_decode(&encoded(&unknown)).is_err());
    let mut invalid = original.clone();
    invalid.rows = BoundedVec::new(Vec::new()).unwrap();
    assert!(checked_decode(&encoded(&invalid)).is_err());
    let mut invalid = original.clone();
    invalid.columns = BoundedVec::new(vec![id("x"), id("x")]).unwrap();
    assert!(checked_decode(&encoded(&invalid)).is_err());
    let mut invalid = original.clone();
    invalid.columns = BoundedVec::new(vec![id("x"), id("y")]).unwrap();
    assert!(checked_decode(&encoded(&invalid)).is_err());
}

#[test]
fn sql_source_invalid_cursor_and_missing_mapping_reject_before_transaction() {
    let original = batch(vec![SqlSourceCell::Int(1)]);
    let mut invalid = original.clone();
    invalid.position = CursorPosition::Sequence(1);
    assert!(checked_decode(&encoded(&invalid)).is_err());
    let mut invalid = original.clone();
    invalid.position = CursorPosition::TimestampMillis(2);
    assert!(checked_decode(&encoded(&invalid)).is_err());
    let mut invalid = original;
    invalid.mapping_descriptor.content = RecordBytes::new(Vec::new()).unwrap();
    assert!(checked_decode(&encoded(&invalid)).is_err());
}

#[test]
fn sql_source_opaque_tokens_advance_without_lexical_ordering() {
    let mut input = batch(vec![SqlSourceCell::Int(1)]);
    input.position = CursorPosition::Opaque {
        cursor_type: "page-v1".into(),
        value: "aaa".into(),
    };
    input.expected_previous = Some(CursorPosition::Opaque {
        cursor_type: "page-v1".into(),
        value: "zzz".into(),
    });
    assert!(SqlSourceBatchRequest::new(input.clone()).is_ok());
    input.position = input.expected_previous.clone().unwrap();
    assert!(SqlSourceBatchRequest::new(input).is_err());
}

#[test]
fn sql_source_collection_and_aggregate_byte_bounds_are_enforced() {
    assert!(
        BoundedVec::<SqlSourceCell, MAX_SQL_SOURCE_COLUMNS>::new(vec![
            SqlSourceCell::Null;
            MAX_SQL_SOURCE_COLUMNS + 1
        ])
        .is_err()
    );
    let row = BoundedVec::new(vec![SqlSourceCell::Null]).unwrap();
    assert!(SqlSourceRows::new(vec![row; MAX_SQL_SOURCE_ROWS + 1]).is_err());
    assert!(SqlSourceText::new("x".repeat(crate::contract::MAX_RECORD_BYTES + 1)).is_err());
    let mut input = batch(vec![SqlSourceCell::Text(
        SqlSourceText::new("x".repeat(crate::contract::MAX_RECORD_BYTES)).unwrap(),
    )]);
    input.rows = BoundedVec::new(vec![input.rows.as_slice()[0].clone(); 17]).unwrap();
    assert!(SqlSourceBatchRequest::new(input).is_err());
}

#[test]
fn sql_source_unknown_cell_and_zero_committed_epoch_are_invalid() {
    assert!(rmp_serde::from_slice::<SqlSourceCell>(&encoded(
        &json!({"kind":"unknown", "value":1})
    ))
    .is_err());
    let request = SqlSourceBatchRequest::new(batch(vec![SqlSourceCell::Int(1)])).unwrap();
    let result = SqlSourceBatchResult {
        canonical_digests: request.canonical_digests().unwrap(),
        accepted_position: CursorPosition::Sequence(2),
        affected_count: 1,
        committed_source_epoch: NonZeroU64::new(9).unwrap(),
        authority_digest: Digest256::from_bytes([3; 32]),
    };
    assert_eq!(
        rmp_serde::from_slice::<SqlSourceBatchResult>(&encoded(&result)).unwrap(),
        result
    );
    let mut invalid = serde_json::to_value(result).unwrap();
    invalid["committed_source_epoch"] = json!(0);
    assert!(serde_json::from_value::<SqlSourceBatchResult>(invalid).is_err());
}

#[cfg(feature = "query")]
#[test]
fn sql_source_method_roundtrip_preserves_checked_typed_batch() {
    let request = SqlSourceBatchRequest::new(batch(vec![SqlSourceCell::Int(i64::MAX)])).unwrap();
    let method = crate::protocol::Method::SqlSourceBatch {
        batch: request.clone(),
    };
    let decoded: crate::protocol::Method = rmp_serde::from_slice(&encoded(&method)).unwrap();
    let crate::protocol::Method::SqlSourceBatch { batch: decoded } = decoded else {
        panic!("expected SQL source batch method");
    };
    assert_eq!(decoded, request);
}
