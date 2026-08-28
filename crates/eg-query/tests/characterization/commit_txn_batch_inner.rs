//! Characterization tests for the private `commit_txn_batch_inner`
//! (CX-EG-01, `crates/eg-query/src/tables/store.rs`), reached only through
//! the public `TableStore::commit_txn_batch`.
//!
//! Fixture mirrors `sql_context_cache_invalidation.rs`'s own `commit()`
//! helper (same crate, same `MutationBatch` shape used by the served
//! `commit_sql_catalog_txn` gateway). Covers: a fresh commit (`replayed:
//! false`, affected-row count, outbox intents from both operations AND an
//! explicit `batch.outbox` entry), an idempotent replay of the identical
//! batch (same result, `replayed: true`), an IDEMPOTENCY_CONFLICT (same
//! idempotency_key, different batch_id/content), and a STALE_VERSION
//! rejection (stale `expected_graph_version`).
//!
//! `commit_txn_batch`/`commit_txn_batch_result` always pass `crashpoint:
//! None` (per commit_txn_batch_inner's own doc: "production always
//! supplies None") -- the four crash-injection branches (BeforeRows,
//! AfterRowsBeforeMetadata, BeforeCommit, AfterCommitBeforeAck) are
//! therefore unreachable through ANY public TableStore method and are not
//! exercised here; they are presumably covered by a dedicated durability/
//! chaos test elsewhere in this crate with test-only access to
//! `commit_txn_batch_inner` directly. Flagged in the lane report.

#![cfg(feature = "sql")]

use eg_query::{Column, ColumnType, TableSchema, TableStore, TableTxn, TxnOp};
use eg_types::mutation_batch::{
    MutationBatch, MutationDomain, MutationOperation, MutationOutboxIntent, MutationRequestContext,
    MutationSurface, MUTATION_BATCH_VERSION,
};

const TENANT: &str = "tenant-commit-txn-batch";
const GRAPH: &str = "graph-commit-txn-batch";

fn schema() -> TableSchema {
    TableSchema::new(
        "widgets",
        vec![Column::new("id", ColumnType::BigInt, false, true)],
    )
}

fn insert_txn() -> TableTxn {
    let mut txn = TableTxn::new();
    txn.push(TxnOp::Insert {
        table: "widgets".into(),
        col_order: vec!["id".into()],
        rows: vec![vec![1i64.into()], vec![2i64.into()]],
    });
    txn
}

/// Mirrors `sql_context_cache_invalidation.rs`'s `commit()` fixture shape.
fn batch(store: &TableStore, batch_id: &str, idempotency_key: &str) -> MutationBatch {
    let expected = store.mutation_version(TENANT, GRAPH).unwrap();
    MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.to_string(),
        context: MutationRequestContext {
            request_id: 1,
            principal: format!("principal:sha256:{}", "a".repeat(64)),
            purpose: None,
            policy_fingerprint: None,
            trace_id: None,
        },
        tenant: TENANT.to_string(),
        graph: GRAPH.to_string(),
        placement_epoch: 0,
        idempotency_key: idempotency_key.to_string(),
        expected_graph_version: Some(expected),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Query,
            domain: MutationDomain::SqlCatalog,
            method: eg_types::protocol::Method::ApplyMutation {
                event_type: "sql_catalog_operation".to_string(),
                query: "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                    .to_string(),
            },
        }],
        outbox: vec![MutationOutboxIntent {
            topic: "engine.projection.rebuild".to_string(),
            key: batch_id.to_string(),
            payload: vec![1],
            headers: Default::default(),
        }],
        created_at_ms: 100,
    }
}

#[test]
fn fresh_commit_applies_rows_and_replay_is_idempotent() {
    let (store, _path) = TableStore::open_temp().expect("temporary table store");
    store.create_table(&schema(), false).expect("create table");

    let b = batch(&store, "batch-1", "idem-1");
    let first = store
        .commit_txn_batch(&insert_txn(), &b, 100)
        .expect("fresh commit");
    assert!(!first.replayed);
    assert_eq!(store.mutation_version(TENANT, GRAPH).unwrap(), 1);
    assert_eq!(store.scan("widgets").unwrap().len(), 2);

    // Identical batch resubmitted: idempotent replay, no double-apply.
    let replay = store
        .commit_txn_batch(&insert_txn(), &b, 100)
        .expect("idempotent replay");
    assert!(replay.replayed);
    assert_eq!(replay.record.batch.batch_id, first.record.batch.batch_id);
    assert_eq!(store.mutation_version(TENANT, GRAPH).unwrap(), 1);
    assert_eq!(store.scan("widgets").unwrap().len(), 2);
}

#[test]
fn same_idempotency_key_different_batch_is_a_conflict() {
    let (store, _path) = TableStore::open_temp().expect("temporary table store");
    store.create_table(&schema(), false).expect("create table");

    let first = batch(&store, "batch-a", "shared-idem-key");
    store
        .commit_txn_batch(&insert_txn(), &first, 100)
        .expect("first commit");

    let second = batch(&store, "batch-b", "shared-idem-key");
    let err = store
        .commit_txn_batch(&insert_txn(), &second, 100)
        .expect_err("different batch under the same idempotency key must conflict");
    assert!(
        err.contains("IDEMPOTENCY_CONFLICT"),
        "unexpected error: {err}"
    );
}

#[test]
fn stale_expected_graph_version_is_rejected() {
    let (store, _path) = TableStore::open_temp().expect("temporary table store");
    store.create_table(&schema(), false).expect("create table");

    let mut stale = batch(&store, "batch-stale", "idem-stale");
    stale.expected_graph_version = Some(stale.expected_graph_version.unwrap() + 1);
    let err = store
        .commit_txn_batch(&insert_txn(), &stale, 100)
        .expect_err("stale expected_graph_version must be rejected");
    assert!(err.contains("STALE_VERSION"), "unexpected error: {err}");
    assert_eq!(store.mutation_version(TENANT, GRAPH).unwrap(), 0);
}
