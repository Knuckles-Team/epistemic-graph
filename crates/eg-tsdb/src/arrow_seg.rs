//! Arrow/columnar segment path — the DataFusion-scannable representation
//! (CONCEPT:AU-KG.retrieval.god-nodes-communities, feature `arrow-seg`).
//!
//! A `Segment` is a contiguous Arrow `RecordBatch` of `(ts: Int64, value: Float64)`
//! for one series — the SAME shape eg-query hands DataFusion for graph rows, so the
//! relational planner can `GROUP BY` / filter a series with NO new executor.
//!
//! ## The easy-vs-native split this path makes concrete
//! * `time_bucket` windowed aggregate is **trivial in DataFusion** — it's
//!   `SELECT (ts/w)*w AS bucket, avg(value) … GROUP BY 1`. Run for real below.
//! * ASOF join + gap-fill are **NOT expressible** in the DataFusion 54 SQL path → they stay
//!   native (`query.rs`).
//!
//! The redb chunk store (`store.rs`) is the durable/append tier; an Arrow segment is
//! the projection of a time window INTO the relational planner — hybrid, not
//! either/or.

#![cfg(feature = "arrow-seg")]

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;

use crate::point::Point;

/// Build an Arrow `RecordBatch` segment `(ts, value)` from stored points (field 0).
pub fn segment_from_points(points: &[Point]) -> RecordBatch {
    let ts: Int64Array = points.iter().map(|p| Some(p.ts)).collect();
    let val: Float64Array = points.iter().map(|p| Some(p.values[0])).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(schema, vec![Arc::new(ts), Arc::new(val)]).expect("segment schema/columns")
}

/// Register a segment as table `series` and run `time_bucket` as plain SQL GROUP BY,
/// returning `(bucket_start, mean_value)` rows — the windowed aggregate with NO
/// native code on the DataFusion leg.
pub async fn time_bucket_sql(points: &[Point], width: i64) -> Result<Vec<(i64, f64)>, String> {
    if width <= 0 {
        return Err("time_bucket width must be positive".into());
    }
    let ctx = SessionContext::new();
    let batch = segment_from_points(points);
    ctx.register_batch("series", batch)
        .map_err(|e| e.to_string())?;
    let sql = format!(
        "SELECT (ts / {w}) * {w} AS bucket, avg(value) AS m \
         FROM series GROUP BY bucket ORDER BY bucket",
        w = width
    );
    let df = ctx.sql(&sql).await.map_err(|e| e.to_string())?;
    let batches = df.collect().await.map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for b in batches {
        let bucket = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("bucket not i64")?;
        let m = b
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or("mean not f64")?;
        for i in 0..b.num_rows() {
            out.push((bucket.value(i), m.value(i)));
        }
    }
    Ok(out)
}
