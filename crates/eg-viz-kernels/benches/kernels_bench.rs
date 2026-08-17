//! V2 kernel benchmarks (D-VZ-1) — proves the complexity claims each kernel's
//! module doc makes: `m4_reduce` is `O(n)` regardless of `width_px`, `lttb_reduce`
//! is `O(n)` once x-sorted. Run at three magnitudes (1e4/1e6/1e8 rows) so a
//! super-linear regression shows up as a non-constant ns/row figure, not just a
//! slow absolute number at one size.
//!
//! `criterion` reports throughput (`Throughput::Elements`) so the printed
//! "time/element" figure is directly comparable across magnitudes — an O(n)
//! kernel's ns/element should stay roughly flat as n grows 1e4 -> 1e6 -> 1e8;
//! an O(n log n) or worse kernel's would visibly climb.
//!
//! Run: `cargo bench -p eg-viz-kernels --target-dir ./target-isolated`
//! (1e8-row cases are slow by construction — this is a deliberate proof point,
//! not a fast CI-loop benchmark; GOC-70's "must pass on a 2-core runner" edict
//! applies to `cargo test`, not `cargo bench`, which is never part of the gate).

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use eg_viz_kernels::{lttb_reduce, m4_reduce};

const MAGNITUDES: [u64; 3] = [10_000, 1_000_000, 100_000_000];

/// Deterministic synthetic series: a noisy sine wave, seeded so every run (and
/// every magnitude) is byte-reproducible without a `rand` dependency (matches
/// this repo's existing `SplitMix64` precedent in
/// `src/server/handlers/viz.rs`).
struct SplitMix64(u64);
impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

fn synthetic_series(n: u64) -> (Vec<f64>, Vec<f64>) {
    let mut rng = SplitMix64(0xC0FF_EE00_D15E_A5E5);
    let xs: Vec<f64> = (0..n).map(|i| i as f64).collect();
    let ys: Vec<f64> = (0..n)
        .map(|i| (i as f64 * 0.001).sin() * 1000.0 + (rng.next_f64() - 0.5) * 10.0)
        .collect();
    (xs, ys)
}

fn bench_m4(c: &mut Criterion) {
    let mut group = c.benchmark_group("m4_reduce");
    for &n in &MAGNITUDES {
        let (xs, ys) = synthetic_series(n);
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                black_box(m4_reduce(
                    black_box(&xs),
                    black_box(&ys),
                    (0.0, n as f64),
                    1920,
                ))
            });
        });
    }
    group.finish();
}

fn bench_lttb(c: &mut Criterion) {
    let mut group = c.benchmark_group("lttb_reduce");
    for &n in &MAGNITUDES {
        let (xs, ys) = synthetic_series(n);
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| black_box(lttb_reduce(black_box(&xs), black_box(&ys), 2000)));
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    // 1e8-row cases are minutes-scale; a small sample count keeps total wall
    // time bounded while still reporting a real mean.
    config = Criterion::default().sample_size(10);
    targets = bench_m4, bench_lttb
}
criterion_main!(benches);
