//! Correctness tests for the eg-tsdb store + every query primitive + the Time-ops
//! sketch. Run: `cargo test -p eg-tsdb --features redb-store`.
#![cfg(feature = "redb-store")]

use eg_tsdb::query::{
    asof_join_backward, decay_weighted_mean, downsample, gap_fill_locf, ohlc_bars, series_ewma,
    time_bucket, Agg,
};
use eg_tsdb::store::{Point, SeriesStore};
use eg_tsdb::time_op::{composed_example, Row, RowSet, TimeOp};
use tempfile::tempdir;

const NS: i64 = 1_000_000_000; // 1 second in ns

fn open() -> (tempfile::TempDir, SeriesStore) {
    let dir = tempdir().unwrap();
    let store = SeriesStore::open(&dir.path().join("ts.redb")).unwrap();
    (dir, store)
}

#[test]
fn store_append_and_range() {
    let (_d, store) = open();
    let pts: Vec<Point> = (0..1000).map(|i| Point::single(i * NS, i as f64)).collect();
    store
        .append_batch("s1", 1, 100 * NS as u64, &["v".into()], &pts)
        .unwrap();

    let meta = store.meta("s1").unwrap().unwrap();
    assert_eq!(meta.count, 1000);
    assert_eq!(meta.min_ts, 0);
    assert_eq!(meta.max_ts, 999 * NS);

    let all = store.scan_all("s1").unwrap();
    assert_eq!(all.len(), 1000);
    assert_eq!(all[0].ts, 0);
    assert_eq!(all[999].values[0], 999.0);

    let r = store.range("s1", 100 * NS, 200 * NS).unwrap();
    assert_eq!(r.len(), 100);
    assert_eq!(r.first().unwrap().ts, 100 * NS);
    assert_eq!(r.last().unwrap().ts, 199 * NS);
}

#[test]
fn store_handles_out_of_order_appends() {
    let (_d, store) = open();
    store
        .append_batch(
            "s",
            1,
            10 * NS as u64,
            &["v".into()],
            &[
                Point::single(5 * NS, 5.0),
                Point::single(NS, 1.0),
                Point::single(3 * NS, 3.0),
            ],
        )
        .unwrap();
    store
        .append_batch(
            "s",
            1,
            10 * NS as u64,
            &["v".into()],
            &[
                Point::single(2 * NS, 2.0), // late, lands between 1 and 3
                Point::single(4 * NS, 4.0),
            ],
        )
        .unwrap();
    let all = store.scan_all("s").unwrap();
    let ts: Vec<i64> = all.iter().map(|p| p.ts / NS).collect();
    assert_eq!(
        ts,
        vec![1, 2, 3, 4, 5],
        "chunk keeps points ts-sorted across late inserts"
    );
}

#[test]
fn store_persists_across_reopen() {
    // Durability: a series written then re-opened scans back identically.
    let dir = tempdir().unwrap();
    let path = dir.path().join("persist.redb");
    {
        let store = SeriesStore::open(&path).unwrap();
        let pts: Vec<Point> = (0..50)
            .map(|i| Point::single(i * NS, (i * 2) as f64))
            .collect();
        store
            .append_batch("p", 1, 10 * NS as u64, &["v".into()], &pts)
            .unwrap();
    }
    let store = SeriesStore::open(&path).unwrap();
    let all = store.scan_all("p").unwrap();
    assert_eq!(all.len(), 50);
    assert_eq!(all[49].values[0], 98.0);
}

#[test]
fn store_rejects_field_width_mismatch() {
    let (_d, store) = open();
    store
        .append_batch(
            "ohlcv",
            2,
            10 * NS as u64,
            &["px".into(), "vol".into()],
            &[Point {
                ts: 0,
                values: vec![1.0, 10.0],
            }],
        )
        .unwrap();
    // A later 1-field append to the 2-field series is a hard error, not silent corruption.
    let err = store.append_batch(
        "ohlcv",
        1,
        10 * NS as u64,
        &["v".into()],
        &[Point::single(NS, 2.0)],
    );
    assert!(err.is_err());
}

#[test]
fn store_evict_before_drops_old_buckets() {
    let (_d, store) = open();
    // 10s buckets; points across [0, 100s).
    let pts: Vec<Point> = (0..100).map(|i| Point::single(i * NS, i as f64)).collect();
    store
        .append_batch("e", 1, 10 * NS as u64, &["v".into()], &pts)
        .unwrap();
    // Drop every bucket ending at-or-before 50s → buckets [0,10),[10,20),...,[40,50).
    let dropped = store.evict_before("e", 50 * NS).unwrap();
    assert_eq!(dropped, 5);
    let all = store.scan_all("e").unwrap();
    assert_eq!(
        all.first().unwrap().ts,
        50 * NS,
        "oldest surviving point is at 50s"
    );
    let meta = store.meta("e").unwrap().unwrap();
    assert_eq!(meta.count, 50);
    assert_eq!(meta.min_ts, 50 * NS);
}

#[test]
fn store_evict_before_trims_straddling_bucket() {
    // CONCEPT:EG-068 — a cutoff INSIDE a bucket trims that bucket point-by-point
    // instead of leaving the straddler intact.
    let (_d, store) = open();
    // 10s buckets; points across [0, 100s).
    let pts: Vec<Point> = (0..100).map(|i| Point::single(i * NS, i as f64)).collect();
    store
        .append_batch("e", 1, 10 * NS as u64, &["v".into()], &pts)
        .unwrap();
    // Cutoff 55s straddles the [50,60) bucket: drops [0,50) wholesale (5 buckets) and
    // trims points 50..55 out of the straddler, leaving 55 as the oldest survivor.
    let dropped = store.evict_before("e", 55 * NS).unwrap();
    assert_eq!(dropped, 5, "five whole buckets [0,10)..[40,50) removed");
    let all = store.scan_all("e").unwrap();
    assert_eq!(
        all.first().unwrap().ts,
        55 * NS,
        "straddling [50,60) bucket trimmed to its >= cutoff points"
    );
    assert!(
        all.iter().all(|p| p.ts >= 55 * NS),
        "no point older than the cutoff survives"
    );
    let meta = store.meta("e").unwrap().unwrap();
    assert_eq!(meta.count, 45, "points 55..100 survive");
    assert_eq!(meta.min_ts, 55 * NS);
    assert_eq!(
        meta.max_ts,
        99 * NS,
        "max unchanged — only old points dropped"
    );
}

#[test]
fn time_bucket_avg() {
    let pts: Vec<Point> = (0..20).map(|i| Point::single(i * NS, i as f64)).collect();
    let buckets = time_bucket(&pts, 10 * NS, Agg::Mean);
    assert_eq!(buckets.len(), 2);
    assert_eq!(buckets[0].bucket_start, 0);
    assert!((buckets[0].value - 4.5).abs() < 1e-9);
    assert!((buckets[1].value - 14.5).abs() < 1e-9);
    assert_eq!(buckets[0].count, 10);
}

#[test]
fn downsample_roundtrips_to_points() {
    let pts: Vec<Point> = (0..20).map(|i| Point::single(i * NS, i as f64)).collect();
    let ds = downsample(&pts, 10 * NS, Agg::Mean);
    assert_eq!(ds.len(), 2);
    assert_eq!(ds[0].ts, 0);
    assert!((ds[1].values[0] - 14.5).abs() < 1e-9);
}

#[test]
fn ohlc_bars_basic() {
    let pts = vec![
        Point {
            ts: NS,
            values: vec![10.0, 1.0],
        },
        Point {
            ts: 2 * NS,
            values: vec![12.0, 1.0],
        },
        Point {
            ts: 3 * NS,
            values: vec![8.0, 2.0],
        },
        Point {
            ts: 4 * NS,
            values: vec![11.0, 1.0],
        },
    ];
    let bars = ohlc_bars(&pts, 10 * NS);
    assert_eq!(bars.len(), 1);
    let b = &bars[0];
    assert_eq!(b.open, 10.0);
    assert_eq!(b.high, 12.0);
    assert_eq!(b.low, 8.0);
    assert_eq!(b.close, 11.0);
    assert_eq!(b.volume, 5.0);
}

#[test]
fn asof_join_backward_nearest_prior() {
    let right = vec![
        Point::single(0, 100.0),
        Point::single(10 * NS, 110.0),
        Point::single(20 * NS, 120.0),
    ];
    let left = vec![
        Point::single(5 * NS, 0.0),  // -> 100 (tick @0)
        Point::single(10 * NS, 0.0), // -> 110 (tick @10, <=)
        Point::single(25 * NS, 0.0), // -> 120 (tick @20)
    ];
    let joined = asof_join_backward(&left, &right, None);
    assert_eq!(joined[0].right, Some(100.0));
    assert_eq!(joined[1].right, Some(110.0));
    assert_eq!(joined[2].right, Some(120.0));
}

#[test]
fn asof_tolerance_drops_stale_match() {
    let right = vec![Point::single(0, 100.0)];
    let left = vec![Point::single(5 * NS, 0.0)];
    let joined = asof_join_backward(&left, &right, Some(NS)); // 1s tol, 5s gap
    assert_eq!(joined[0].right, None);
}

#[test]
fn gap_fill_locf_carries_forward() {
    let pts = vec![Point::single(0, 1.0), Point::single(30 * NS, 3.0)];
    let grid = gap_fill_locf(&pts, 0, 40 * NS, 10 * NS);
    let vals: Vec<Option<f64>> = grid.iter().map(|g| g.value).collect();
    assert_eq!(vals, vec![Some(1.0), Some(1.0), Some(1.0), Some(3.0)]);
    assert!(!grid[0].filled); // real obs at t=0
    assert!(grid[1].filled); // carried forward
    assert!(!grid[3].filled); // real obs at t=30s
}

#[test]
fn decay_weighted_mean_recency() {
    let half_life = 1.0; // 1 second
    let now = 5 * NS;
    let pts = vec![Point::single(0, 0.0), Point::single(now, 100.0)];
    let m = decay_weighted_mean(&pts, now, half_life);
    assert!(m > 95.0, "recent obs dominates: {m}");
    let m2 = decay_weighted_mean(&pts, now, 1e9);
    assert!((m2 - 50.0).abs() < 1.0, "long half-life ~ unweighted: {m2}");
}

#[test]
fn decay_weighted_mean_uses_shared_curve() {
    // The series decay must equal the shared eg_core curve at the half-life point.
    let now = 10 * NS;
    let half_life_secs = 10.0;
    // One obs exactly one half-life old (v=100) + one at `now` (v=0).
    let pts = vec![Point::single(0, 100.0), Point::single(now, 0.0)];
    // weights: 0.5 (age=10s=half_life) and 1.0 → mean = 0.5*100 / (0.5+1.0) = 33.33…
    let m = decay_weighted_mean(&pts, now, half_life_secs);
    assert!(
        (m - (50.0 / 1.5)).abs() < 1e-6,
        "shared-curve weighting: {m}"
    );
}

#[test]
fn finance_kernel_reuse_ewma() {
    let pts: Vec<Point> = (0..10).map(|i| Point::single(i * NS, i as f64)).collect();
    let ewma = series_ewma(&pts, 3);
    assert_eq!(ewma.len(), 10);
    assert_eq!(ewma[0], 0.0);
    assert!(ewma[9] > ewma[5] && ewma[5] > ewma[1]);
}

#[test]
fn time_op_asof_over_rowset() {
    let (_d, store) = open();
    let ticks: Vec<Point> = (0..10)
        .map(|i| Point::single(i * NS, 100.0 + i as f64))
        .collect();
    store
        .append_batch("price", 1, 100 * NS as u64, &["px".into()], &ticks)
        .unwrap();

    let rs = RowSet::from_timed(vec![("m1".into(), 3 * NS), ("m2".into(), 7 * NS)]);
    let out = TimeOp::Asof {
        store: &store,
        series_id: "price",
        tolerance: None,
    }
    .apply(rs)
    .unwrap();
    let s: Vec<Option<f32>> = out.rows.iter().map(|r| r.score).collect();
    assert_eq!(s, vec![Some(103.0), Some(107.0)]);
}

#[test]
fn composed_filter_traverse_asof_rank_example() {
    // The time leg of filter→traverse→asof→rank.
    let (_d, store) = open();
    let ticks: Vec<Point> = (0..100).map(|i| Point::single(i * NS, i as f64)).collect();
    store
        .append_batch("px", 1, 50 * NS as u64, &["px".into()], &ticks)
        .unwrap();

    let handoff = RowSet {
        rows: vec![
            Row {
                id: "old_high".into(),
                score: None,
                event_ts: Some(10 * NS),
            }, // px=10, old
            Row {
                id: "recent_low".into(),
                score: None,
                event_ts: Some(90 * NS),
            }, // px=90, recent
        ],
    };
    let now = 100 * NS;
    let half_life = 5.0; // 5s — recency matters
    let ranked = composed_example(handoff, &store, "px", now, half_life, 10).unwrap();
    assert_eq!(ranked.rows[0].id, "recent_low");
    assert!(ranked.rows[0].score.unwrap() > ranked.rows[1].score.unwrap());
}

// ── measured-ish smoke: append+scan a non-trivial series and assert correctness +
//    that the path is not pathologically slow. The spike measured ~3.4M append /
//    ~7.4M scan pts/s on a dev box `--release`; THIS runs in a debug test binary, so
//    the floors are conservative (debug is ~10-50× slower). The point is to catch an
//    O(n²) regression (e.g. RMW of an ever-growing single chunk per point), NOT to
//    benchmark — for real numbers use the `tsdb_bench` bin under `--release`.
//
//    Buckets are sized 1h with points at 1Hz (the spike's profile) so the run spreads
//    across many chunks and append is bounded by per-bucket RMW, not one giant chunk.
#[test]
fn append_and_scan_throughput_sane() {
    use std::time::Instant;
    let (_d, store) = open();
    let n = 200_000usize;
    let hz_ns = NS; // 1 point/second
    let bucket_ns = 3_600 * NS as u64; // 1h buckets → ~3600 pts/chunk
    let pts: Vec<Point> = (0..n)
        .map(|i| Point::single(i as i64 * hz_ns, i as f64))
        .collect();
    let t0 = Instant::now();
    for chunk in pts.chunks(10_000) {
        store
            .append_batch("b", 1, bucket_ns, &["v".into()], chunk)
            .unwrap();
    }
    let append_s = t0.elapsed().as_secs_f64();
    let t1 = Instant::now();
    let all = store.scan_all("b").unwrap();
    let scan_s = t1.elapsed().as_secs_f64();
    assert_eq!(all.len(), n);
    assert_eq!(all[n - 1].values[0], (n - 1) as f64);
    let append_rate = n as f64 / append_s;
    let scan_rate = n as f64 / scan_s;
    // Conservative debug-build floors (an O(n²) regression blows past these by orders).
    assert!(
        append_rate > 50_000.0,
        "append rate too low: {append_rate:.0} pts/s"
    );
    assert!(
        scan_rate > 500_000.0,
        "scan rate too low: {scan_rate:.0} pts/s"
    );
}
