//! The PromQL barrier functions: the σ → 0 limit is `predict_linear`'s crossing,
//! a noisy series gets a probability strictly between the certain outcomes, and
//! the function names are served by the evaluator.

use super::super::{query_instant, MemSeriesSource, Value};
use super::SECOND;

fn disk(points: &[(i64, f64)]) -> MemSeriesSource {
    let mut source = MemSeriesSource::new();
    source.push(
        MemSeriesSource::labels("disk_free_bytes", &[("host", "a")]),
        points.iter().map(|(t, v)| (t * SECOND, *v)).collect(),
    );
    source
}

fn scalar(source: &MemSeriesSource, expr: &str, at: i64) -> f64 {
    let Value::Instant(samples) = query_instant(source, expr, at * SECOND).unwrap() else {
        panic!("instant vector expected");
    };
    assert_eq!(samples.len(), 1, "{expr}");
    assert!(!samples[0].labels.contains_key("__name__"));
    samples[0].value
}

#[test]
fn a_linear_series_crosses_where_predict_linear_says() {
    // 100 → 40 over 60 s: -1 per second, so 0 is reached 40 s after the last point.
    let source = disk(&[(0, 100.0), (20, 80.0), (40, 60.0), (60, 40.0)]);
    let crossing = scalar(
        &source,
        "time_to_exhaustion(disk_free_bytes[60s], 0, 0.5)",
        60,
    );
    assert!((crossing - 40.0).abs() < 1e-9, "{crossing}");
    let linear = scalar(&source, "predict_linear(disk_free_bytes[60s], 40)", 60);
    assert!(
        linear.abs() < 1e-9,
        "predict_linear at the crossing: {linear}"
    );
    assert_eq!(
        scalar(
            &source,
            "barrier_hit_probability(disk_free_bytes[60s], 0, 39)",
            60
        ),
        0.0
    );
    assert_eq!(
        scalar(
            &source,
            "barrier_hit_probability(disk_free_bytes[60s], 0, 40)",
            60
        ),
        1.0
    );
}

#[test]
fn a_noisy_series_gets_an_intermediate_probability_and_a_finite_quantile() {
    let source = disk(&[
        (0, 100.0),
        (10, 93.0),
        (20, 90.0),
        (30, 79.0),
        (40, 77.0),
        (50, 66.0),
        (60, 62.0),
    ]);
    let p = scalar(
        &source,
        "barrier_hit_probability(disk_free_bytes[60s], 0, 60)",
        60,
    );
    assert!(p > 0.0 && p < 1.0, "{p}");
    let later = scalar(
        &source,
        "barrier_hit_probability(disk_free_bytes[60s], 0, 600)",
        60,
    );
    assert!(later > p);
    let median = scalar(
        &source,
        "time_to_exhaustion(disk_free_bytes[60s], 0, 0.5)",
        60,
    );
    assert!(median.is_finite() && median > 0.0);
}

#[test]
fn a_receding_series_never_exhausts_at_a_high_quantile() {
    let source = disk(&[(0, 40.0), (20, 60.0), (40, 80.0), (60, 100.0)]);
    let t = scalar(
        &source,
        "time_to_exhaustion(disk_free_bytes[60s], 0, 0.9)",
        60,
    );
    assert!(t.is_infinite());
}
