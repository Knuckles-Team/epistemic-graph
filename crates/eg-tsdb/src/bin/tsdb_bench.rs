//! Measured eg-tsdb benchmark (a plain bin, NOT criterion — keeps deps minimal).
//! Run: `cargo run -p eg-tsdb --release --bin tsdb_bench -- [n_points] [flush_batch]`
//!
//! Reports: append throughput, full-scan throughput, range-scan latency, on-disk
//! footprint (bytes/point), and the cost of each query primitive over N points.

use std::time::Instant;

use eg_tsdb::query::{asof_join_backward, decay_weighted_mean, gap_fill_locf, time_bucket, Agg};
use eg_tsdb::store::{Point, SeriesStore};

const NS: i64 = 1_000_000_000;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n: usize = args
        .get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let flush: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10_000);
    let bucket_secs: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(3600); // 1h buckets

    let dir = std::env::temp_dir().join(format!("tsdb_bench_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("ts.redb");
    let store = SeriesStore::open(&path).unwrap();

    let bucket_ns = bucket_secs * NS as u64;
    // 1 point per second of wall-clock; field0 = a noisy ramp.
    println!("== eg-tsdb bench: n={n} flush_batch={flush} bucket={bucket_secs}s ==");

    // ---- APPEND ----
    let t0 = Instant::now();
    let mut buf: Vec<Point> = Vec::with_capacity(flush);
    for i in 0..n {
        let ts = (i as i64) * NS; // 1Hz
        let v = (i as f64).sin() * 10.0 + i as f64 * 0.001;
        buf.push(Point::single(ts, v));
        if buf.len() == flush {
            store
                .append_batch("bench", 1, bucket_ns, &["v".into()], &buf)
                .unwrap();
            buf.clear();
        }
    }
    if !buf.is_empty() {
        store
            .append_batch("bench", 1, bucket_ns, &["v".into()], &buf)
            .unwrap();
    }
    let append_dt = t0.elapsed();
    let apps = n as f64 / append_dt.as_secs_f64();
    println!(
        "append : {n} pts in {:.3}s -> {:.0} pts/s ({:.2} M/s)",
        append_dt.as_secs_f64(),
        apps,
        apps / 1e6
    );

    // ---- FOOTPRINT ----
    let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    println!(
        "disk   : {} bytes total -> {:.2} bytes/point (raw point = 16 B: i64 ts + f64 val)",
        bytes,
        bytes as f64 / n as f64
    );

    // ---- FULL SCAN ----
    let t1 = Instant::now();
    let all = store.scan_all("bench").unwrap();
    let scan_dt = t1.elapsed();
    println!(
        "scan   : {} pts in {:.3}s -> {:.0} pts/s ({:.2} M/s)",
        all.len(),
        scan_dt.as_secs_f64(),
        all.len() as f64 / scan_dt.as_secs_f64(),
        all.len() as f64 / scan_dt.as_secs_f64() / 1e6
    );
    assert_eq!(all.len(), n);

    // ---- RANGE SCAN (1% window) ----
    let win = (n / 100).max(1) as i64;
    let from = (n as i64 / 2) * NS;
    let to = from + win * NS;
    let t2 = Instant::now();
    let r = store.range("bench", from, to).unwrap();
    let range_dt = t2.elapsed();
    println!(
        "range  : {} pts (1% window) in {:.3}ms",
        r.len(),
        range_dt.as_secs_f64() * 1e3
    );

    // ---- QUERY PRIMITIVES over the scanned series ----
    let t3 = Instant::now();
    let buckets = time_bucket(&all, 60 * NS, Agg::Mean); // 1-min bars
    let tb_dt = t3.elapsed();
    println!(
        "time_bucket(1min): {} bars in {:.3}ms",
        buckets.len(),
        tb_dt.as_secs_f64() * 1e3
    );

    // asof: join the series against 10k random events
    let events: Vec<Point> = (0..10_000)
        .map(|i| Point::single((i as i64 * (n as i64 / 10_000)) * NS, 0.0))
        .collect();
    let t4 = Instant::now();
    let joined = asof_join_backward(&events, &all, None);
    let asof_dt = t4.elapsed();
    println!(
        "asof_join(10k events vs {n} ticks): {} rows in {:.3}ms",
        joined.len(),
        asof_dt.as_secs_f64() * 1e3
    );

    // gap_fill on a 1-min grid over the whole range
    let t5 = Instant::now();
    let grid = gap_fill_locf(&all, 0, n as i64 * NS, 60 * NS);
    let gf_dt = t5.elapsed();
    println!(
        "gap_fill(1min grid): {} grid rows in {:.3}ms",
        grid.len(),
        gf_dt.as_secs_f64() * 1e3
    );

    // decay-weighted mean as of now (composition with the memory curve)
    let t6 = Instant::now();
    let dwm = decay_weighted_mean(&all, n as i64 * NS, 30.0 * 86400.0);
    let dw_dt = t6.elapsed();
    println!(
        "decay_weighted_mean (30d half-life): {:.4} in {:.3}ms",
        dwm,
        dw_dt.as_secs_f64() * 1e3
    );

    let _ = std::fs::remove_dir_all(&dir);
}
