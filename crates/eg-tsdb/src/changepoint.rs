//! Change-point detection over a `Vec<Point>` series (CONCEPT:EG-KG.temporal depth:
//! per-modality change-point detection) — pure-Rust, no new deps, works in the
//! lean/Pi (no-DataFusion) build exactly like [`crate::query`]'s other native
//! primitives.
//!
//! Two detectors, the two standard families:
//!
//!   * [`cusum_change_points`] / [`cusum_detect`] — the classic two-sided CUSUM
//!     control chart: O(n), online-capable (one pass, constant memory), tuned by a
//!     target mean + drift/threshold. Good for a STREAMING mean-shift alarm.
//!   * [`pelt_change_points`] — an exact optimal-partitioning dynamic program
//!     (Killick et al. 2012's PELT cost recursion, WITHOUT the pruning inequality —
//!     hence "-lite": O(n²) exact, not the amortised O(n) pruned variant) over a
//!     Gaussian-mean-shift cost. Good for OFFLINE "where are ALL the regime changes
//!     in this historical series" analysis.
//!
//! [`join_change_points_to_events`] is the root-cause join: correlate a series'
//! change points to graph-linked events (a deploy, an incident, any timestamped
//! node id) by REUSING [`crate::query::asof_join_backward`] — the same
//! nearest-in-time primitive `eg-tsdb` already uses for series↔event alignment —
//! rather than re-implementing a second nearest-neighbour search.

use crate::point::{Point, Ts};
use crate::query::asof_join_backward;

/// Two-sided CUSUM change-point detector over field 0 of a ts-sorted series.
/// Tracks the running positive (`s_pos`) and negative (`s_neg`) cumulative
/// deviation from `target_mean`; an index is flagged the instant either
/// accumulator exceeds `threshold`, after which BOTH reset to zero (the classic
/// "restart after alarm" CUSUM discipline, so a sustained shift is flagged once
/// per shift rather than at every subsequent step). `drift` (>= 0) is the
/// per-step slack allowed before accumulation starts — a small drift (e.g. half
/// the smallest shift you care about) keeps ordinary noise from alarming.
pub fn cusum_change_points(points: &[Point], target_mean: f64, drift: f64, threshold: f64) -> Vec<usize> {
    let mut s_pos = 0.0f64;
    let mut s_neg = 0.0f64;
    let mut out = Vec::new();
    for (i, p) in points.iter().enumerate() {
        let x = p.values[0];
        s_pos = (s_pos + x - target_mean - drift).max(0.0);
        s_neg = (s_neg + target_mean - x - drift).max(0.0);
        if s_pos > threshold || s_neg > threshold {
            out.push(i);
            s_pos = 0.0;
            s_neg = 0.0;
        }
    }
    out
}

/// Self-tuning CUSUM: estimate `target_mean`/`drift`/`threshold` from the first
/// `warmup` points (assumed in-control) and run [`cusum_change_points`] over the
/// WHOLE series (including the warmup window itself, so a shift starting inside
/// it is still caught). `drift = 0.5*sigma`, `threshold = k_sigma*sigma` — the
/// standard CUSUM tuning recipe. Empty warmup (or `warmup == 0`) ⇒ no detection.
pub fn cusum_detect(points: &[Point], warmup: usize, k_sigma: f64) -> Vec<usize> {
    let warm = warmup.min(points.len());
    if warm == 0 {
        return Vec::new();
    }
    let vals: Vec<f64> = points[..warm].iter().map(|p| p.values[0]).collect();
    let mean = vals.iter().sum::<f64>() / vals.len() as f64;
    let var = vals.iter().map(|v| (v - mean) * (v - mean)).sum::<f64>() / vals.len() as f64;
    let sigma = var.sqrt().max(1e-9);
    cusum_change_points(points, mean, 0.5 * sigma, k_sigma * sigma)
}

/// PELT-lite: exact optimal-partitioning change-point detection over field 0 of a
/// series. Segment cost = sum of squared deviations from the segment mean (the
/// standard Gaussian-mean-shift cost), computed in O(1) per candidate segment from
/// prefix sums, so the full O(n²) dynamic program (every split point × every
/// candidate previous split) still runs in O(n²) time / O(n) space — "lite" means
/// this skips PELT's pruning inequality (which gets the same EXACT answer down to
/// amortised O(n) on well-behaved cost functions); correctness is identical, only
/// the asymptotic speed differs. `penalty` (>= 0) is the per-change-point
/// complexity cost — larger ⇒ fewer, more conservative change points. A BIC-style
/// `2.0 * (n as f64).ln()` is a reasonable default when no domain prior exists.
/// Returns the (0-based, ascending) indices where a new regime BEGINS (excludes
/// index 0 and `n`, which are never "change points" — they're the series bounds).
pub fn pelt_change_points(points: &[Point], penalty: f64) -> Vec<usize> {
    let n = points.len();
    if n < 2 {
        return Vec::new();
    }
    let vals: Vec<f64> = points.iter().map(|p| p.values[0]).collect();

    // Prefix sums / sum-of-squares so any segment [s, e)'s cost is O(1).
    let mut psum = vec![0.0f64; n + 1];
    let mut psumsq = vec![0.0f64; n + 1];
    for i in 0..n {
        psum[i + 1] = psum[i] + vals[i];
        psumsq[i + 1] = psumsq[i] + vals[i] * vals[i];
    }
    let segment_cost = |s: usize, e: usize| -> f64 {
        let len = (e - s) as f64;
        let sum = psum[e] - psum[s];
        let sumsq = psumsq[e] - psumsq[s];
        let mean = sum / len;
        // sum((x - mean)^2) = sumsq - 2*mean*sum + len*mean^2, clamped at 0 to
        // absorb floating-point noise on a near-constant segment.
        (sumsq - 2.0 * mean * sum + len * mean * mean).max(0.0)
    };

    // f[t] = min total cost of optimally partitioning points[0..t); cp[t] = the
    // last change point used to achieve it (for backtracking).
    let mut f = vec![0.0f64; n + 1];
    let mut cp = vec![0usize; n + 1];
    f[0] = -penalty; // cancels the +penalty the first real segment [0, t) adds.
    for t in 1..=n {
        let mut best = f64::INFINITY;
        let mut best_s = 0usize;
        for s in 0..t {
            let c = f[s] + segment_cost(s, t) + penalty;
            if c < best {
                best = c;
                best_s = s;
            }
        }
        f[t] = best;
        cp[t] = best_s;
    }

    let mut out = Vec::new();
    let mut t = n;
    while t > 0 {
        let s = cp[t];
        if s > 0 {
            out.push(s);
        }
        t = s;
    }
    out.reverse();
    out
}

/// One change point tied (or not) to the nearest graph-linked event — the
/// root-cause join.
#[derive(Clone, Debug, PartialEq)]
pub struct CausalChangePoint {
    /// The change point's own timestamp.
    pub ts: Ts,
    /// Its index into the source series.
    pub index: usize,
    /// The id of the nearest-in-time event at-or-before `ts` (within tolerance),
    /// or `None` if no event qualifies.
    pub linked_node: Option<String>,
    /// That event's own timestamp, when linked.
    pub link_ts: Option<Ts>,
}

/// Correlate detected change points (indices into `points`, as returned by
/// [`cusum_change_points`]/[`cusum_detect`]/[`pelt_change_points`]) to graph-linked
/// `events` — `(ts, node_id)` pairs such as deploy/incident nodes — by REUSING
/// [`asof_join_backward`]: each change point is matched to the nearest event
/// at-or-before it, within `tolerance` ns (`None` = unbounded). `events` need not
/// be pre-sorted; this sorts a copy internally. This is the "why did this metric
/// shift" bridge from a bare index to an explaining graph node, not a new
/// nearest-neighbour implementation.
pub fn join_change_points_to_events(
    points: &[Point],
    change_indices: &[usize],
    events: &[(Ts, String)],
    tolerance: Option<i64>,
) -> Vec<CausalChangePoint> {
    if events.is_empty() {
        return change_indices
            .iter()
            .map(|&i| CausalChangePoint {
                ts: points[i].ts,
                index: i,
                linked_node: None,
                link_ts: None,
            })
            .collect();
    }

    // Left side: one Point per change-point timestamp (value unused).
    let left: Vec<Point> = change_indices
        .iter()
        .map(|&i| Point::single(points[i].ts, 0.0))
        .collect();

    // Right side: events sorted by ts, each carrying its ORIGINAL index as the
    // ASOF value (the same index-encoding trick `query::sensor_fuse` uses) so the
    // matched row can look the node id back up after the join.
    let mut sorted_events: Vec<(Ts, usize)> = events
        .iter()
        .enumerate()
        .map(|(idx, (ts, _))| (*ts, idx))
        .collect();
    sorted_events.sort_by_key(|(ts, _)| *ts);
    let right: Vec<Point> = sorted_events
        .iter()
        .map(|(ts, idx)| Point::single(*ts, *idx as f64))
        .collect();

    let joined = asof_join_backward(&left, &right, tolerance);

    change_indices
        .iter()
        .zip(joined.iter())
        .map(|(&i, row)| CausalChangePoint {
            ts: points[i].ts,
            index: i,
            linked_node: row.right.map(|v| events[v as usize].1.clone()),
            link_ts: row.right_ts,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series(vals: &[f64]) -> Vec<Point> {
        vals.iter()
            .enumerate()
            .map(|(i, &v)| Point::single(i as i64 * 1_000_000_000, v))
            .collect()
    }

    #[test]
    fn cusum_flags_a_sustained_mean_shift() {
        // 50 points around 0.0, then a sustained shift to 10.0.
        let mut vals: Vec<f64> = vec![0.0; 50];
        vals.extend(vec![10.0; 50]);
        let points = series(&vals);
        let hits = cusum_change_points(&points, 0.0, 1.0, 5.0);
        assert!(!hits.is_empty(), "a 10-unit sustained shift must be flagged");
        // The first flagged index should land near the true shift at 50.
        assert!(
            hits[0] >= 48 && hits[0] <= 56,
            "flagged index {} should be near the true shift point 50",
            hits[0]
        );
    }

    #[test]
    fn cusum_stays_quiet_on_a_flat_series() {
        let vals: Vec<f64> = vec![1.0; 100];
        let points = series(&vals);
        let hits = cusum_change_points(&points, 1.0, 0.5, 3.0);
        assert!(hits.is_empty(), "a flat series must not alarm");
    }

    #[test]
    fn cusum_detect_auto_tunes_from_warmup() {
        let mut vals: Vec<f64> = (0..40).map(|_| 0.0).collect();
        vals.extend(vec![20.0; 40]);
        let points = series(&vals);
        let hits = cusum_detect(&points, 40, 4.0);
        assert!(!hits.is_empty(), "auto-tuned CUSUM should catch the shift");
    }

    #[test]
    fn pelt_finds_the_true_two_segment_boundary() {
        let mut vals: Vec<f64> = vec![0.0; 60];
        vals.extend(vec![15.0; 60]);
        let points = series(&vals);
        let cps = pelt_change_points(&points, 2.0 * (points.len() as f64).ln());
        assert!(!cps.is_empty(), "must detect the mean shift");
        let nearest = cps.iter().min_by_key(|&&c| (c as i64 - 60).abs()).unwrap();
        assert!(
            (*nearest as i64 - 60).abs() <= 3,
            "detected change point {nearest} should be within 3 of the true boundary at 60: {cps:?}"
        );
    }

    #[test]
    fn pelt_finds_no_change_points_in_a_flat_series() {
        let vals: Vec<f64> = vec![5.0; 50];
        let points = series(&vals);
        let cps = pelt_change_points(&points, 2.0 * (points.len() as f64).ln());
        assert!(cps.is_empty(), "a constant series has no regime change");
    }

    #[test]
    fn pelt_higher_penalty_yields_fewer_or_equal_change_points() {
        let mut vals: Vec<f64> = vec![0.0; 30];
        vals.extend(vec![5.0; 30]);
        vals.extend(vec![-5.0; 30]);
        vals.extend(vec![8.0; 30]);
        let points = series(&vals);
        let lenient = pelt_change_points(&points, 1.0);
        let strict = pelt_change_points(&points, 500.0);
        assert!(
            strict.len() <= lenient.len(),
            "a larger penalty must not find MORE change points: strict={} lenient={}",
            strict.len(),
            lenient.len()
        );
    }

    #[test]
    fn root_cause_join_links_change_point_to_nearest_prior_event() {
        let vals: Vec<f64> = {
            let mut v = vec![0.0; 20];
            v.extend(vec![50.0; 20]);
            v
        };
        let points = series(&vals);
        let change_idx = 20usize; // ts = 20e9 ns
        let events = vec![
            (19_500_000_000i64, "deploy:v1.2.3".to_string()),
            (5_000_000_000i64, "deploy:v1.0.0".to_string()),
        ];
        let joined = join_change_points_to_events(&points, &[change_idx], &events, Some(2_000_000_000));
        assert_eq!(joined.len(), 1);
        assert_eq!(joined[0].linked_node.as_deref(), Some("deploy:v1.2.3"));
    }

    #[test]
    fn root_cause_join_yields_none_when_no_event_in_tolerance() {
        let points = series(&[0.0, 1.0, 2.0, 30.0]);
        let events = vec![(0i64, "old-deploy".to_string())];
        // Change at index 3 (ts=3e9); event is at ts=0, far outside a 1s tolerance.
        let joined = join_change_points_to_events(&points, &[3], &events, Some(1_000_000_000));
        assert_eq!(joined[0].linked_node, None);
    }

    #[test]
    fn root_cause_join_with_no_events_yields_all_unlinked() {
        let points = series(&[0.0, 1.0, 2.0]);
        let joined = join_change_points_to_events(&points, &[1, 2], &[], None);
        assert!(joined.iter().all(|c| c.linked_node.is_none()));
    }
}
