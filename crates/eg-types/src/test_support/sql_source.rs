//! The canonical SQL source batch fixture shared by the wire, owner and served
//! handler tests: one provider stream (`jira` / `issues`, partition `project-a`)
//! with a first position and bounded descriptor and mapping content.

use crate::change_envelope::CursorPosition;
use crate::contract::{BoundedVec, Digest256, RecordBytes, ResourceId};
use crate::storage_wire::{
    SqlSourceBatch, SqlSourceBatchRequest, SqlSourceCell, SqlSourceDescriptor, SqlSourceJson,
    SqlSourceMappingDescriptor, SqlSourceText,
};

pub fn id(value: &str) -> ResourceId {
    ResourceId::new(value).unwrap()
}

/// The target table and the schema version/digest the batch was prepared against.
pub struct SqlSourceTarget<'a> {
    pub table: &'a str,
    pub columns: &'a [&'a str],
    pub schema_version: u64,
    pub schema_digest: Digest256,
}

/// One first-position batch appending `rows` to `target`.
pub fn batch(target: &SqlSourceTarget<'_>, rows: Vec<Vec<SqlSourceCell>>) -> SqlSourceBatch {
    SqlSourceBatch {
        source: id("jira"),
        partition: SqlSourceText::new("project-a".into()).unwrap(),
        position: CursorPosition::Sequence(1),
        expected_previous: None,
        source_descriptor: SqlSourceDescriptor {
            provider: id("jira"),
            dataset: id("issues"),
            metadata: SqlSourceJson::new(serde_json::json!({"deployment":"internal"})).unwrap(),
        },
        mapping_descriptor: SqlSourceMappingDescriptor {
            format: id("json"),
            content: RecordBytes::new(br#"{"issue_id":"id"}"#.to_vec()).unwrap(),
        },
        table: id(target.table),
        columns: BoundedVec::new(target.columns.iter().map(|name| id(name)).collect()).unwrap(),
        rows: BoundedVec::new(
            rows.into_iter()
                .map(|row| BoundedVec::new(row).unwrap())
                .collect(),
        )
        .unwrap(),
        expected_schema_version: target.schema_version,
        expected_schema_digest: target.schema_digest,
    }
}

/// A checked copy of `request` with `edit` applied.
pub fn change(
    request: &SqlSourceBatchRequest,
    edit: impl FnOnce(&mut SqlSourceBatch),
) -> SqlSourceBatchRequest {
    let mut batch = request.as_batch().clone();
    edit(&mut batch);
    SqlSourceBatchRequest::new(batch).unwrap()
}
