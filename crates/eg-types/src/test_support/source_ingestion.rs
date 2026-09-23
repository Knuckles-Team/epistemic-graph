//! One valid `SourceIngest` request, shared by the digest vector and the
//! method-body vectors so both pin the value connectors actually send.

use crate::contract::{BoundedVec, Digest256, ResourceId};
use crate::source_ingestion::{
    SourceCheckpoint, SourceIngestionBatch, SourceIngestionMode, SourceIngestionRequest,
    SourceJson, SourceRecord, SourceRecordProvenance,
};

/// A delta batch with one record and a previous checkpoint.
pub fn request() -> SourceIngestionRequest {
    let connector = ResourceId::new("demo-connector").expect("canonical connector id");
    let stream = ResourceId::new("items").expect("canonical stream id");
    SourceIngestionRequest::new(SourceIngestionBatch {
        connector: connector.clone(),
        mode: SourceIngestionMode::Delta,
        strict_schema: true,
        records: BoundedVec::new(vec![SourceRecord {
            stream: stream.clone(),
            record_id: "item-1".into(),
            mapping_reference: "manifest:demo-connector#schema_mappings/item".into(),
            payload: SourceJson::new(serde_json::json!({"name": "one", "id": 1}))
                .expect("bounded source JSON"),
            updated_at: Some("2026-09-20T00:00:00Z".into()),
            provenance: SourceRecordProvenance {
                connector,
                adapter_kind: ResourceId::new("mcp").expect("canonical adapter id"),
                server: "demo-server".into(),
                tool: "list_items".into(),
                tool_schema_sha256: Digest256::from_bytes([7; 32]),
                source_uri: "demo://items/item-1".into(),
            },
        }])
        .expect("bounded source records"),
        relationships: BoundedVec::new(Vec::new()).expect("bounded source relationships"),
        provider_checkpoint: SourceCheckpoint {
            stream: stream.clone(),
            position: SourceJson::new(serde_json::json!({"page": 2})).expect("bounded cursor JSON"),
            content_hash: None,
            watermark: Some("2026-09-20T00:00:00Z".into()),
            pending_watermark: None,
        },
        expected_previous_checkpoint: Some(SourceCheckpoint {
            stream,
            position: SourceJson::new(serde_json::json!({"page": 1}))
                .expect("bounded previous cursor JSON"),
            content_hash: None,
            watermark: None,
            pending_watermark: None,
        }),
        authoritative_live_ids: None,
        empty_authoritative_approval: None,
        withdrawals: BoundedVec::new(Vec::new()).expect("bounded source withdrawals"),
    })
    .expect("valid source ingestion request")
}
