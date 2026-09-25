//! EH-529: the streaming left profile equals its batch recompute, survives a checkpoint
//! split bit for bit, and re-anchors without drift over a long stream.

use super::{left_profile_batch, ANCHOR_EVERY};
use crate::series::{apply, Spec, State};

fn walk(n: usize, seed: u64) -> Vec<f64> {
    let mut state = seed;
    let mut level = 1e6;
    (0..n)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            level += ((state >> 33) % 21) as f64 / 10.0 - 1.0;
            level
        })
        .collect()
}

fn close(a: &[Option<f64>], b: &[Option<f64>]) {
    assert_eq!(a.len(), b.len());
    for (i, (x, y)) in a.iter().zip(b).enumerate() {
        match (x, y) {
            (Some(x), Some(y)) => assert!((x - y).abs() < 1e-7, "at {i}: {x} vs {y}"),
            _ => assert_eq!(x.is_some(), y.is_some(), "at {i}"),
        }
    }
}

#[test]
fn streaming_equals_the_batch_left_profile() {
    for (n, m, history) in [(300, 8, 50), (200, 5, 1000), (150, 12, 20)] {
        let xs = walk(n, n as u64);
        let streamed = apply(Spec::LeftProfile { m, history }, &xs).unwrap();
        close(&streamed, &left_profile_batch(&xs, m, history));
    }
}

#[test]
fn a_long_stream_re_anchors_without_drift() {
    let n = 3 * ANCHOR_EVERY + 77;
    let xs = walk(n, 5);
    let streamed = apply(Spec::LeftProfile { m: 6, history: 64 }, &xs).unwrap();
    close(&streamed, &left_profile_batch(&xs, 6, 64));
}

#[test]
fn a_restored_checkpoint_continues_bit_for_bit() {
    let xs = walk(260, 9);
    let spec = Spec::LeftProfile { m: 7, history: 40 };
    let bits = |v: Vec<Option<f64>>| v.into_iter().map(|o| o.map(f64::to_bits)).collect::<Vec<_>>();
    let whole = bits(apply(spec, &xs).unwrap());
    for split in [0, 6, 7, 55, 259] {
        let mut state = State::new(spec).unwrap();
        let mut got: Vec<Option<f64>> = xs[..split].iter().map(|&x| state.step(Some(x), None)).collect();
        let mut restored: State = rmp_serde::from_slice(&rmp_serde::to_vec(&state).unwrap()).unwrap();
        got.extend(xs[split..].iter().map(|&x| restored.step(Some(x), None)));
        assert_eq!(bits(got), whole, "split at {split}");
    }
}

#[test]
fn a_repeated_window_is_at_distance_zero_and_a_break_stands_out() {
    let period: Vec<f64> = (0..10).map(|i| f64::from(i * i % 7)).collect();
    let mut xs: Vec<f64> = period.iter().cycle().take(100).copied().collect();
    xs[80] = 40.0;
    let profile = apply(Spec::LeftProfile { m: 10, history: 60 }, &xs).unwrap();
    assert!(profile[40].is_some_and(|d| d < 1e-6), "{:?}", profile[40]);
    let peak = profile[80..90].iter().flatten().fold(0.0f64, |a, &b| a.max(b));
    assert!(peak > 1.0, "the break is far from every earlier window: {peak}");
}
