//! The DataFusion segment path: time_bucket as plain SQL GROUP BY.
//! Run: `cargo test -p eg-tsdb --features arrow-seg`.
#![cfg(feature = "arrow-seg")]

use eg_tsdb::arrow_seg::time_bucket_sql;
use eg_tsdb::point::Point;

const NS: i64 = 1_000_000_000;

#[tokio::test]
async fn time_bucket_via_datafusion_groupby() {
    let pts: Vec<Point> = (0..20).map(|i| Point::single(i * NS, i as f64)).collect();
    let rows = time_bucket_sql(&pts, 10 * NS).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, 0);
    assert!((rows[0].1 - 4.5).abs() < 1e-9);
    assert!((rows[1].1 - 14.5).abs() < 1e-9);
}
