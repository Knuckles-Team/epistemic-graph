use serde_json::json;

use super::*;

fn digest(byte: u8) -> Digest256 {
    Digest256::from_bytes([byte; 32])
}

fn request() -> SourceIngestionBatch {
    SourceIngestionBatch {
        connector: ResourceId::new("demo-connector").unwrap(),
        mapping_reference: "manifest:demo-connector#schema_mappings/item".into(),
        records: BoundedVec::new(vec![SourceRecord {
            stream: ResourceId::new("items").unwrap(),
            record_id: "item-1".into(),
            payload: SourceJson::new(json!({"name": "one", "id": 1})).unwrap(),
            updated_at: Some("2026-09-20T00:00:00Z".into()),
            provenance: SourceRecordProvenance {
                connector: ResourceId::new("demo-connector").unwrap(),
                adapter_kind: ResourceId::new("mcp").unwrap(),
                server: "demo-server".into(),
                tool: "list_items".into(),
                tool_schema_sha256: digest(7),
                source_uri: "demo://items/item-1".into(),
            },
        }])
        .unwrap(),
        cursor: SourceCursor {
            stream: ResourceId::new("items").unwrap(),
            position: SourceJson::new(json!({"page": 2})).unwrap(),
            watermark: Some("2026-09-20T00:00:00Z".into()),
            pending_watermark: None,
        },
        expected_previous_cursor: Some(SourceCursor {
            stream: ResourceId::new("items").unwrap(),
            position: SourceJson::new(json!({"page": 1})).unwrap(),
            watermark: None,
            pending_watermark: None,
        }),
    }
}

#[test]
fn canonical_digest_is_stable_across_payload_key_order() {
    let first = SourceIngestionRequest::new(request()).unwrap();
    let mut reordered = request();
    reordered.records = BoundedVec::new(vec![SourceRecord {
        payload: SourceJson::new(json!({"id": 1, "name": "one"})).unwrap(),
        ..reordered.records.as_slice()[0].clone()
    }])
    .unwrap();
    let second = SourceIngestionRequest::new(reordered).unwrap();
    assert_eq!(
        first.batch_digest().unwrap(),
        second.batch_digest().unwrap()
    );
}

#[test]
fn rejects_duplicate_source_identity() {
    let mut batch = request();
    batch.records = BoundedVec::new(vec![
        batch.records.as_slice()[0].clone(),
        batch.records.as_slice()[0].clone(),
    ])
    .unwrap();
    assert!(SourceIngestionRequest::new(batch)
        .unwrap_err()
        .contains("duplicate source identity"));
}

#[test]
fn rejects_cross_connector_provenance_and_cross_stream_cursor() {
    let mut connector = request();
    let mut record = connector.records.as_slice()[0].clone();
    record.provenance.connector = ResourceId::new("other").unwrap();
    connector.records = BoundedVec::new(vec![record]).unwrap();
    assert!(SourceIngestionRequest::new(connector)
        .unwrap_err()
        .contains("provenance connector"));

    let mut stream = request();
    stream.cursor.stream = ResourceId::new("other-stream").unwrap();
    stream.expected_previous_cursor = None;
    assert!(SourceIngestionRequest::new(stream)
        .unwrap_err()
        .contains("record stream"));
}

#[test]
fn request_wire_never_contains_mapping_content_or_authority_fields() {
    let value = serde_json::to_value(SourceIngestionRequest::new(request()).unwrap()).unwrap();
    let object = value.as_object().unwrap();
    assert!(object.contains_key("mapping_reference"));
    assert!(!object.contains_key("mapping"));
    assert!(!object.contains_key("tenant"));
    assert!(!object.contains_key("principal"));
    assert!(!object.contains_key("mutation"));
    assert!(object["records"][0]["payload"].is_object());
    assert!(object["cursor"]["position"].is_object());
    assert!(object["expected_previous_cursor"]["position"].is_object());
}

#[test]
fn named_msgpack_projects_source_json_as_objects_not_binary_wrappers() {
    let request = SourceIngestionRequest::new(request()).unwrap();
    let wire: serde_json::Value =
        rmp_serde::from_slice(&request.canonical_bytes().unwrap()).unwrap();
    assert!(wire["records"][0]["payload"].is_object());
    assert!(wire["cursor"]["position"].is_object());
    assert!(wire["expected_previous_cursor"]["position"].is_object());
}

#[test]
fn initial_cursor_emits_nil_while_optional_leaves_are_omitted() {
    let mut batch = request();
    batch.expected_previous_cursor = None;
    let mut record = batch.records.as_slice()[0].clone();
    record.updated_at = None;
    batch.records = BoundedVec::new(vec![record]).unwrap();
    batch.cursor.watermark = None;
    batch.cursor.pending_watermark = None;
    let request = SourceIngestionRequest::new(batch).unwrap();
    let wire: serde_json::Value =
        rmp_serde::from_slice(&request.canonical_bytes().unwrap()).unwrap();

    assert_eq!(
        wire.get("expected_previous_cursor"),
        Some(&serde_json::Value::Null)
    );
    assert!(wire["records"][0].get("updated_at").is_none());
    assert!(wire["cursor"].get("watermark").is_none());
    assert!(wire["cursor"].get("pending_watermark").is_none());
}
