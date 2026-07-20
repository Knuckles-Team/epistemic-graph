//! STEP 0 de-risk spike (CONCEPT:EG-KG.query.read-only-sql-query): can DataFusion 54's async executor be
//! driven to completion on a CURRENT-THREAD tokio runtime created INSIDE a
//! spawn_blocking task (the established `compute_off_lock` idiom)?
//!
//! The engine server runs on a multi-thread Tokio reactor; running DataFusion
//! directly on a reactor worker would block it. The established pattern is to push
//! CPU-heavy off-lock work to the blocking pool. DataFusion's `collect()` is async,
//! so the blocking task must own a runtime to drive it. We prove the smallest case
//! here before building the real surface.
#![cfg(feature = "sql")]

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

/// Run `sql` on a current-thread runtime built inside this (already-blocking)
/// thread, mirroring what the real handler does inside `compute_off_lock`.
fn run_on_current_thread(sql: &'static str) -> Result<usize, String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("runtime build: {e}"))?;
    rt.block_on(async move {
        let ctx = SessionContext::new();

        // A tiny in-memory MemTable: one Int64 column `id` with three rows.
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
        )
        .map_err(|e| format!("record batch: {e}"))?;
        let table =
            MemTable::try_new(schema, vec![vec![batch]]).map_err(|e| format!("mem table: {e}"))?;
        ctx.register_table("t", Arc::new(table))
            .map_err(|e| format!("register: {e}"))?;

        let df = ctx.sql(sql).await.map_err(|e| format!("sql: {e}"))?;
        let batches = df.collect().await.map_err(|e| format!("collect: {e}"))?;
        Ok(batches.iter().map(|b| b.num_rows()).sum())
    })
}

#[test]
fn datafusion_runs_on_current_thread_in_spawn_blocking() {
    // Outer multi-thread runtime == the server reactor.
    let outer = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let n = outer.block_on(async {
        tokio::task::spawn_blocking(|| run_on_current_thread("SELECT 1 AS one"))
            .await
            .expect("blocking task joined")
            .expect("sql executed")
    });
    assert_eq!(n, 1, "SELECT 1 should yield one row");

    let rows = outer.block_on(async {
        tokio::task::spawn_blocking(|| run_on_current_thread("SELECT id FROM t WHERE id > 1"))
            .await
            .expect("blocking task joined")
            .expect("sql executed")
    });
    assert_eq!(rows, 2, "two rows have id > 1");
}
