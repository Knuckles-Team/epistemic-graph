//! Bound SQL stores and genuine operation envelopes for source publication tests.

use super::*;
use crate::tables::store::dev_scope_grant;
use eg_types::contract::{BoundedVec, Digest256, MethodId, Nonce, RecordBytes, ResourceId};
use eg_types::mutation_batch::{
    method_schema_id, CompiledOperation, CompiledScope, DurabilityDomain, MutationEnvelope,
    MutationOperation, MutationOutboxIntent, MutationSurface, VersionExpectation,
    BATCH_COMPILED_METHODS, MUTATION_BATCH_VERSION,
};
use eg_types::storage_wire::{SqlSourceDescriptor, SqlSourceJson, SqlSourceMappingDescriptor};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) struct Fixture {
    store: Option<TableStore>,
    directory: PathBuf,
}

impl Fixture {
    pub(super) fn new(schema: &TableSchema) -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "eg-source-adapter-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir(&directory).unwrap();
        let store = TableStore::open_scoped(
            directory.join("sql.redb"),
            "tenant-a",
            dev_scope_grant::dev_verifier(),
            dev_scope_grant::DEV_PRINCIPAL,
            dev_scope_grant::DEV_PROOF,
        )
        .unwrap();
        store.create_table(schema, false).unwrap();
        Self {
            store: Some(store),
            directory,
        }
    }

    pub(super) fn path(&self) -> PathBuf {
        self.directory.join("sql.redb")
    }

    pub(super) fn close(&mut self) {
        drop(self.store.take());
    }

    pub(super) fn reopen(&mut self) {
        self.store = Some(
            TableStore::open_scoped(
                self.path(),
                "tenant-a",
                dev_scope_grant::dev_verifier(),
                dev_scope_grant::DEV_PRINCIPAL,
                dev_scope_grant::DEV_PROOF,
            )
            .unwrap(),
        );
    }

    pub(super) fn store(&self) -> &TableStore {
        self.store.as_ref().unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.store.take());
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

pub(super) fn schema() -> TableSchema {
    TableSchema::new(
        "issues",
        vec![
            Column::new("id", ColumnType::BigInt, false, true),
            Column::new("title", ColumnType::Text, true, false),
        ],
    )
}

pub(super) fn id(value: &str) -> ResourceId {
    ResourceId::new(value.to_string()).unwrap()
}

pub(super) fn request(schema: &TableSchema, cells: Vec<SqlSourceCell>) -> SqlSourceBatchRequest {
    SqlSourceBatchRequest::new(SqlSourceBatch {
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
        table: id(&schema.name),
        columns: BoundedVec::new(
            schema
                .columns()
                .iter()
                .take(cells.len())
                .map(|column| id(&column.name))
                .collect(),
        )
        .unwrap(),
        rows: BoundedVec::new(vec![BoundedVec::new(cells).unwrap()]).unwrap(),
        expected_schema_version: 0,
        expected_schema_digest: Digest256::parse(&schema.schema_digest().unwrap()).unwrap(),
    })
    .unwrap()
}

pub(super) fn change(
    request: &SqlSourceBatchRequest,
    mutate: impl FnOnce(&mut SqlSourceBatch),
) -> SqlSourceBatchRequest {
    let mut batch = request.as_batch().clone();
    mutate(&mut batch);
    SqlSourceBatchRequest::new(batch).unwrap()
}

pub(super) fn next(
    request: &SqlSourceBatchRequest,
    position: u64,
    row_id: i64,
) -> SqlSourceBatchRequest {
    change(request, |batch| {
        batch.expected_previous = Some(batch.position.clone());
        batch.position = CursorPosition::Sequence(position);
        batch.rows = BoundedVec::new(vec![BoundedVec::new(vec![
            SqlSourceCell::Int(row_id),
            SqlSourceCell::Text(SqlSourceText::new("next".into()).unwrap()),
        ])
        .unwrap()])
        .unwrap();
    })
}

pub(super) fn batch(
    request: &SqlSourceBatchRequest,
    key: &str,
    resource: &str,
    expected: u64,
) -> MutationBatch {
    batch_in_tenant(request, key, resource, expected, "tenant-a")
}

pub(super) fn batch_in_tenant(
    request: &SqlSourceBatchRequest,
    key: &str,
    resource: &str,
    expected: u64,
    tenant: &str,
) -> MutationBatch {
    let identity = super::super::super::authority::sql_scope_identity(tenant, resource).unwrap();
    let method = MethodId::new(BATCH_COMPILED_METHODS).unwrap();
    let envelope = MutationEnvelope::for_scope(
        CompiledScope {
            identity: &identity,
            actor: dev_scope_grant::DEV_PRINCIPAL,
            serving_principal: dev_scope_grant::DEV_PRINCIPAL,
            request_id: 7,
            idempotency_key: key,
            nonce: Nonce::minted(),
            now_ms: 100,
        },
        CompiledOperation {
            method_schema_id: method_schema_id(&method).unwrap(),
            method,
            method_schema_digest: Digest256::from_bytes([0; 32]),
            canonical_payload_digest: Digest256::from_bytes([1; 32]),
        },
    )
    .unwrap();
    let mut batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: format!("batch-{key}"),
        envelope,
        identity,
        placement_epoch: 0,
        version_expectation: VersionExpectation::Native(expected),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Query,
            domain: DurabilityDomain::SqlCatalog,
            method: Method::SqlSourceBatch {
                batch: request.clone(),
            },
        }],
        outbox: vec![MutationOutboxIntent {
            topic: "engine.projection.rebuild".into(),
            key: "issues".into(),
            payload: vec![1],
            headers: BTreeMap::from([("actor".into(), dev_scope_grant::DEV_PRINCIPAL.into())]),
        }],
        created_at_ms: 100,
    };
    batch
        .reseal_envelope(Digest256::from_bytes([0; 32]))
        .unwrap();
    batch
}

pub(super) fn result(commit: &MutationBatchCommit) -> SqlSourceBatchResult {
    eg_storage::decode_ledger_record(commit.record.result_msgpack.as_ref().unwrap()).unwrap()
}

pub(super) fn epoch(store: &TableStore) -> u64 {
    let read = store.authority.read().unwrap();
    store.authority.source_snapshot(&read).unwrap().epoch
}

pub(super) fn checkpoint_bytes(
    store: &TableStore,
    request: &SqlSourceBatchRequest,
) -> Option<Vec<u8>> {
    let read = store.authority.read().unwrap();
    let table = read
        .open_owner_table(eg_storage::SQL_SOURCE_CHECKPOINTS)
        .unwrap();
    let request = request.as_batch();
    table
        .get((
            "tenant-a",
            request.source.as_str(),
            request.partition.as_str(),
        ))
        .unwrap()
        .map(|row| row.value().to_vec())
}
