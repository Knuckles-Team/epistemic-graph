//! Bound SQL stores and genuine operation envelopes for source publication tests.

use super::*;
use crate::tables::store::dev_scope_grant;
use eg_types::contract::{BoundedVec, Digest256, MethodId, Nonce};
use eg_types::mutation_batch::{
    method_schema_id, CompiledOperation, CompiledScope, DurabilityDomain, MutationEnvelope,
    MutationOperation, MutationOutboxIntent, MutationSurface, VersionExpectation,
    BATCH_COMPILED_METHODS, MUTATION_BATCH_VERSION,
};
use eg_types::test_support::sql_source;
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

fn schema() -> TableSchema {
    TableSchema::new(
        "issues",
        vec![
            Column::new("id", ColumnType::BigInt, false, true),
            Column::new("title", ColumnType::Text, true, false),
        ],
    )
}

pub(super) use eg_types::test_support::sql_source::{change, id};

pub(super) fn request(schema: &TableSchema, cells: Vec<SqlSourceCell>) -> SqlSourceBatchRequest {
    let columns: Vec<&str> = schema
        .columns()
        .iter()
        .take(cells.len())
        .map(|column| column.name.as_str())
        .collect();
    let target = sql_source::SqlSourceTarget {
        table: &schema.name,
        columns: &columns,
        schema_version: 0,
        schema_digest: Digest256::parse(&schema.schema_digest().unwrap()).unwrap(),
    };
    SqlSourceBatchRequest::new(sql_source::batch(&target, vec![cells])).unwrap()
}

/// An id table whose omitted `expanded` column defaults to `default_len` bytes.
pub(super) fn omitted_default_schema(default_len: usize) -> TableSchema {
    let mut omitted = Column::new("expanded", ColumnType::Text, false, false);
    omitted.default = Some(serde_json::json!("x".repeat(default_len)));
    TableSchema::new(
        "issues",
        vec![Column::new("id", ColumnType::BigInt, false, true), omitted],
    )
}

/// The standard two-column table with one `(1, NULL)` request against it.
pub(super) fn seeded() -> (Fixture, SqlSourceBatchRequest) {
    let schema = schema();
    let fixture = Fixture::new(&schema);
    let request = request(&schema, vec![SqlSourceCell::Int(1), SqlSourceCell::Null]);
    (fixture, request)
}

/// Commit `request` as the stream's first batch on scope `source-a`.
pub(super) fn commit_first(
    store: &TableStore,
    request: &SqlSourceBatchRequest,
    key: &str,
) -> MutationBatchCommit {
    store
        .commit_source_batch(&batch(request, key, "source-a", 0), 101)
        .unwrap()
}

/// A single-row row set.
pub(super) fn one_row(cells: Vec<SqlSourceCell>) -> eg_types::storage_wire::SqlSourceRows {
    BoundedVec::new(vec![BoundedVec::new(cells).unwrap()]).unwrap()
}

/// `request` carrying `count` single-cell rows with ids `1..=count`.
pub(super) fn with_id_rows(request: &SqlSourceBatchRequest, count: i64) -> SqlSourceBatchRequest {
    change(request, |batch| {
        batch.rows = BoundedVec::new(
            (1..=count)
                .map(|id| BoundedVec::new(vec![SqlSourceCell::Int(id)]).unwrap())
                .collect(),
        )
        .unwrap();
    })
}

pub(super) fn next(
    request: &SqlSourceBatchRequest,
    position: u64,
    row_id: i64,
) -> SqlSourceBatchRequest {
    change(request, |batch| {
        batch.expected_previous = Some(batch.position.clone());
        batch.position = CursorPosition::Sequence(position);
        batch.rows = one_row(vec![
            SqlSourceCell::Int(row_id),
            SqlSourceCell::Text(SqlSourceText::new("next".into()).unwrap()),
        ]);
    })
}

/// Every durable effect of one publication attempt, for all-or-nothing checks.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Publication {
    pub(super) epoch: u64,
    pub(super) rows: usize,
    pub(super) checkpoint: bool,
    pub(super) receipt: bool,
    pub(super) outbox: usize,
}

impl Publication {
    /// The state after `rows` published rows, with or without this attempt's
    /// own checkpoint, receipt and outbox record.
    pub(super) fn expected(epoch: u64, rows: usize, published: bool) -> Self {
        Self {
            epoch,
            rows,
            checkpoint: published,
            receipt: published,
            outbox: usize::from(published),
        }
    }

    pub(super) fn observe(
        store: &TableStore,
        request: &SqlSourceBatchRequest,
        batch: &MutationBatch,
    ) -> Self {
        Self {
            epoch: epoch(store),
            rows: store.scan(request.as_batch().table.as_str()).unwrap().len(),
            checkpoint: checkpoint_bytes(store, request).is_some(),
            receipt: store
                .mutation_batch(&batch.identity, &batch.batch_id)
                .unwrap()
                .is_some(),
            outbox: store
                .mutation_outbox(&batch.identity, &batch.batch_id)
                .unwrap()
                .len(),
        }
    }
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
    let publication = MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Query,
        domain: DurabilityDomain::SqlCatalog,
        method: Method::SqlSourceBatch {
            batch: request.clone(),
        },
    };
    let mut batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: format!("batch-{key}"),
        identity,
        envelope,
        operations: vec![publication],
        version_expectation: VersionExpectation::Native(expected),
        placement_epoch: 0,
        fencing_token: None,
        authoritative_state: None,
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
