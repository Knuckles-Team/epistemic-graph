//! EH-562 benchmark: the exact O(1) rolling deviation against the O(window) recompute
//! it replaced. Each case advances a series of `STEPS` points past warm-up with window
//! `w`; the exact kernel's time per step stays flat as `w` grows 16 → 8192, the
//! recompute's grows linearly with `w`.
//!
//! Run: `cargo bench -p eg-numeric --bench rolling_exact`.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use eg_numeric::series::window::Moments;
use eg_numeric::series::{apply, Rolling, Spec};

const WINDOWS: [usize; 4] = [16, 128, 1024, 8192];
const STEPS: usize = 4096;

/// A deterministic walk around a large level (the cancellation-prone regime).
fn series(n: usize) -> Vec<f64> {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    (0..n)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            1e9 + ((state >> 40) % 1000) as f64 * 1e-3
        })
        .collect()
}

/// The O(window) form: a two-pass recompute of each full window.
fn recompute_std(xs: &[f64], w: usize) -> Vec<f64> {
    xs.windows(w)
        .map(|win| Moments::of(win.iter().copied()).map_or(f64::NAN, |m| m.population_std()))
        .collect()
}

fn bench_rolling_std(c: &mut Criterion) {
    let mut group = c.benchmark_group("rolling_std");
    group.sample_size(10);
    group.throughput(Throughput::Elements(STEPS as u64));
    for &w in &WINDOWS {
        let xs = series(STEPS + w);
        group.bench_with_input(BenchmarkId::new("exact_o1", w), &w, |b, &w| {
            b.iter(|| black_box(apply(Spec::Rolling(Rolling::Std, w), black_box(&xs))))
        });
        group.bench_with_input(BenchmarkId::new("recompute_ow", w), &w, |b, &w| {
            b.iter(|| black_box(recompute_std(black_box(&xs), w)))
        });
    }
    group.finish();
}

criterion_group!(benches, bench_rolling_std);
criterion_main!(benches);
