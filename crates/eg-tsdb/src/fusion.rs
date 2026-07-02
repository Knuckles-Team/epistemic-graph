//! Multi-rate sensor-stream alignment for multimodal fusion (CONCEPT:EG-098).
//!
//! The TIME half of robotics/IoT sensor fusion: time-align N scalar sensor series that
//! are sampled at DIFFERENT rates (e.g. a 1 kHz IMU, a 10 Hz GPS, a 30 Hz camera-scalar)
//! onto ONE common target timestamp grid, each channel with its own interpolation mode:
//!
//!   * [`InterpMode::Nearest`]  — the closest sample in time (either side).
//!   * [`InterpMode::Linear`]   — linear interpolation between the two bracketing samples.
//!   * [`InterpMode::AsofHold`] — last-known value at-or-before the instant (forward-fill /
//!     zero-order hold), REUSING the eg-tsdb backward-ASOF primitive
//!     ([`crate::query::asof_join_backward`], the EG-067 seam) verbatim.
//!
//! The output [`AlignedFrame`] (a shared `grid` + one `Option<f64>` column per stream,
//! `None` = a GAP) is pure time-series — NO tensor / Arrow / redb dep, so it lives in the
//! lean/Pi build exactly like the rest of `query`. The MODALITY half — stacking an
//! `AlignedFrame` into a `[timesteps × channels]` eg-tensor `Tensor` frame + a validity
//! mask, plus the windowed (per-window tensor) variant — lives in `eg-tensor::fusion`,
//! which depends on THIS module. Composes eg-tsdb ASOF (EG-067) + EG-085 tensors.

use crate::point::{Point, Ts};
use crate::query::asof_join_backward;

/// Per-channel resample interpolation mode (CONCEPT:EG-098). Chosen per stream because
/// modalities differ: a pose is `Linear`-interpolable, a discrete mode/label wants
/// `AsofHold`, a noisy raw reading may want `Nearest`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InterpMode {
    /// Nearest sample in time to the grid instant (either side). Ties resolve to the
    /// EARLIER (prior) sample for determinism.
    Nearest,
    /// Linear interpolation between the two samples bracketing the grid instant. A grid
    /// instant OUTSIDE the sample span is a GAP (`None`) — no extrapolation.
    Linear,
    /// Last-known value at-or-before the grid instant (forward-fill / zero-order hold),
    /// reusing the backward-ASOF primitive. A grid instant before the first sample is a
    /// GAP (`None`).
    AsofHold,
}

/// One input stream to align (CONCEPT:EG-098): a channel `name`, its ts-sorted `points`
/// (field 0 = the scalar reading), the per-channel [`InterpMode`], and an optional
/// staleness `tolerance` in ns (`None` = unbounded). `points` MUST be ascending in `ts`
/// (as they come out of the store) — the merge/bisection relies on it.
#[derive(Clone, Debug, PartialEq)]
pub struct StreamSpec {
    pub name: String,
    pub points: Vec<Point>,
    pub mode: InterpMode,
    pub tolerance: Option<i64>,
}

impl StreamSpec {
    /// Convenience constructor.
    pub fn new(
        name: impl Into<String>,
        points: Vec<Point>,
        mode: InterpMode,
        tolerance: Option<i64>,
    ) -> Self {
        Self {
            name: name.into(),
            points,
            mode,
            tolerance,
        }
    }
}

/// One aligned channel (CONCEPT:EG-098): its `name` + one `Option<f64>` per grid instant,
/// `None` marking a GAP (no usable sample within tolerance / outside the span). `values`
/// always has the same length as the [`AlignedFrame`]'s `grid`.
#[derive(Clone, Debug, PartialEq)]
pub struct AlignedChannel {
    pub name: String,
    pub values: Vec<Option<f64>>,
}

/// N channels resampled onto ONE shared `grid` (CONCEPT:EG-098): the common time base +
/// one aligned column per input stream, in input order. This is the pure-time-series
/// hand-off that `eg-tensor::fusion` stacks into a fused tensor frame.
#[derive(Clone, Debug, PartialEq)]
pub struct AlignedFrame {
    pub grid: Vec<Ts>,
    pub channels: Vec<AlignedChannel>,
}

/// Resample a ts-sorted scalar series (field 0 of each [`Point`]) onto `grid` under
/// `mode`, bounding each match by `tolerance` ns (`None` = unbounded). Returns one
/// `Option<f64>` per grid instant — `None` is a GAP. `grid` MUST be ascending; an empty
/// `points` yields all-`None` (CONCEPT:EG-098).
pub fn resample(
    points: &[Point],
    grid: &[Ts],
    mode: InterpMode,
    tolerance: Option<i64>,
) -> Vec<Option<f64>> {
    if points.is_empty() {
        return vec![None; grid.len()];
    }
    match mode {
        // Zero-order hold IS the backward-ASOF join over the grid as the left clock —
        // reuse the primitive verbatim (EG-067 seam) rather than re-implement the merge.
        InterpMode::AsofHold => {
            let left: Vec<Point> = grid.iter().map(|&t| Point::single(t, 0.0)).collect();
            asof_join_backward(&left, points, tolerance)
                .into_iter()
                .map(|row| row.right)
                .collect()
        }
        InterpMode::Nearest => grid.iter().map(|&t| nearest(points, t, tolerance)).collect(),
        InterpMode::Linear => grid.iter().map(|&t| linear(points, t, tolerance)).collect(),
    }
}

/// Nearest-in-time sample (ties → the earlier sample), or `None` if the closest sample is
/// farther than `tolerance`.
fn nearest(points: &[Point], t: Ts, tolerance: Option<i64>) -> Option<f64> {
    // First index whose ts >= t (points ascending in ts).
    let idx = points.partition_point(|p| p.ts < t);
    let mut best: Option<(i64, f64)> = None; // (distance-ns, value)
    if idx < points.len() {
        best = Some(((points[idx].ts - t).abs(), points[idx].values[0]));
    }
    if idx > 0 {
        let d = (t - points[idx - 1].ts).abs();
        // `<=` so a tie prefers the earlier (prior) sample.
        match best {
            Some((bd, _)) if d <= bd => best = Some((d, points[idx - 1].values[0])),
            None => best = Some((d, points[idx - 1].values[0])),
            _ => {}
        }
    }
    best.and_then(|(d, v)| match tolerance {
        Some(tol) if d > tol => None,
        _ => Some(v),
    })
}

/// Linear interpolation between the two samples bracketing `t`; `None` outside the span
/// (no extrapolation), or if the bracketing gap exceeds `tolerance`.
fn linear(points: &[Point], t: Ts, tolerance: Option<i64>) -> Option<f64> {
    let idx = points.partition_point(|p| p.ts < t); // first ts >= t
    if idx < points.len() && points[idx].ts == t {
        return Some(points[idx].values[0]); // exact hit
    }
    if idx == 0 || idx >= points.len() {
        return None; // before the first / after the last sample → gap
    }
    let (pt, pv) = (points[idx - 1].ts, points[idx - 1].values[0]);
    let (nt, nv) = (points[idx].ts, points[idx].values[0]);
    if let Some(tol) = tolerance {
        if nt - pt > tol {
            return None; // don't interpolate across a gap wider than tolerance
        }
    }
    if nt == pt {
        return Some(pv);
    }
    let frac = (t - pt) as f64 / (nt - pt) as f64;
    Some(pv + (nv - pv) * frac)
}

/// Time-align N multi-rate `streams` onto a shared `grid` (CONCEPT:EG-098): each stream
/// is [`resample`]d under its own mode/tolerance, yielding one [`AlignedChannel`] per
/// stream in input order. This is the pure-time-series fusion primitive; `eg-tensor`
/// turns the result into a dense tensor frame + mask.
pub fn align_multirate(streams: &[StreamSpec], grid: &[Ts]) -> AlignedFrame {
    let channels = streams
        .iter()
        .map(|s| AlignedChannel {
            name: s.name.clone(),
            values: resample(&s.points, grid, s.mode, s.tolerance),
        })
        .collect();
    AlignedFrame {
        grid: grid.to_vec(),
        channels,
    }
}

/// A uniform target grid: `from`, `from+step`, … strictly `< to` (CONCEPT:EG-098).
/// `step <= 0` ⇒ empty.
pub fn uniform_grid(from: Ts, to: Ts, step: Ts) -> Vec<Ts> {
    let mut g = Vec::new();
    if step <= 0 {
        return g;
    }
    let mut t = from;
    while t < to {
        g.push(t);
        t += step;
    }
    g
}

/// The tumbling-window START timestamps spanning `[from, to)` under EG-067 window
/// semantics — each aligned to `width` as `(t/width)*width` (matching `time_bucket`).
/// `width <= 0` or `from >= to` ⇒ empty (CONCEPT:EG-098). Assumes non-negative ts
/// (epoch-ns), consistent with `time_bucket`.
pub fn tumbling_window_starts(from: Ts, to: Ts, width: Ts) -> Vec<Ts> {
    let mut g = Vec::new();
    if width <= 0 || from >= to {
        return g;
    }
    let mut w = (from / width) * width;
    while w < to {
        g.push(w);
        w += width;
    }
    g
}

#[cfg(test)]
mod eg098_align_tests {
    use super::*;

    const NS: i64 = 1_000_000_000; // 1 second in ns

    fn scalars(pairs: &[(i64, f64)]) -> Vec<Point> {
        pairs.iter().map(|&(t, v)| Point::single(t, v)).collect()
    }

    /// EG-098: Nearest resample picks the closest sample in time (ties → earlier).
    #[test]
    fn eg098_resample_nearest_picks_closest() {
        // samples @ 0=10, 10=20, 20=30 (in ns units for clarity)
        let pts = scalars(&[(0, 10.0), (10, 20.0), (20, 30.0)]);
        let grid = vec![0, 3, 6, 15, 20, 25];
        let out = resample(&pts, &grid, InterpMode::Nearest, None);
        // 0→10 (exact); 3→closer to 0 (d3<d7)→10; 6→closer to 10 (d4<d6)→20;
        // 15→tie(d5,d5)→earlier=20; 20→30 (exact); 25→nearest 20→30.
        assert_eq!(
            out,
            vec![
                Some(10.0),
                Some(10.0),
                Some(20.0),
                Some(20.0),
                Some(30.0),
                Some(30.0)
            ]
        );
    }

    /// EG-098: Nearest honours tolerance — a too-far closest sample is a gap.
    #[test]
    fn eg098_resample_nearest_tolerance_gap() {
        let pts = scalars(&[(0, 10.0), (100, 20.0)]);
        // grid @ 50 is 50 away from both; tolerance 40 → gap.
        let out = resample(&pts, &[50], InterpMode::Nearest, Some(40));
        assert_eq!(out, vec![None]);
        // tolerance 60 → nearest (tie→earlier) = 10.
        let out2 = resample(&pts, &[50], InterpMode::Nearest, Some(60));
        assert_eq!(out2, vec![Some(10.0)]);
    }

    /// EG-098: Linear resample interpolates between the two bracketing samples and
    /// returns a gap outside the sample span (no extrapolation).
    #[test]
    fn eg098_resample_linear_interpolates_between() {
        // samples @ 0=0, 10=100
        let pts = scalars(&[(0, 0.0), (10, 100.0)]);
        let grid = vec![-5, 0, 2, 5, 10, 15];
        let out = resample(&pts, &grid, InterpMode::Linear, None);
        assert_eq!(
            out,
            vec![
                None,         // before span
                Some(0.0),    // exact
                Some(20.0),   // 2/10 → 20
                Some(50.0),   // 5/10 → 50
                Some(100.0),  // exact
                None,         // after span
            ]
        );
    }

    /// EG-098: Linear refuses to interpolate across a bracket gap wider than tolerance.
    #[test]
    fn eg098_resample_linear_tolerance_gap() {
        let pts = scalars(&[(0, 0.0), (100, 100.0)]);
        // bracket width 100 > tol 50 → gap at t=50.
        assert_eq!(resample(&pts, &[50], InterpMode::Linear, Some(50)), vec![None]);
        // tol 150 ≥ 100 → interpolate to 50.
        assert_eq!(
            resample(&pts, &[50], InterpMode::Linear, Some(150)),
            vec![Some(50.0)]
        );
    }

    /// EG-098: AsofHold forward-fills the last-known value; before the first sample is a
    /// gap. Reuses the backward-ASOF machinery.
    #[test]
    fn eg098_resample_asof_hold_forward_fills() {
        let pts = scalars(&[(10 * NS, 5.0), (30 * NS, 9.0)]);
        let grid = vec![0, 10 * NS, 20 * NS, 30 * NS, 40 * NS];
        let out = resample(&pts, &grid, InterpMode::AsofHold, None);
        assert_eq!(
            out,
            vec![
                None,       // before first sample
                Some(5.0),  // exact
                Some(5.0),  // held forward
                Some(9.0),  // exact
                Some(9.0),  // held forward
            ]
        );
    }

    /// EG-098: AsofHold honours tolerance — a value held too long is a gap.
    #[test]
    fn eg098_resample_asof_hold_tolerance_gap() {
        let pts = scalars(&[(0, 5.0)]);
        // hold from 0; at t=2s with a 1s tolerance the held value is too stale → gap.
        let out = resample(&pts, &[0, NS, 2 * NS], InterpMode::AsofHold, Some(NS));
        assert_eq!(out, vec![Some(5.0), Some(5.0), None]);
    }

    /// EG-098: align_multirate resamples N different-rate streams onto ONE grid, each
    /// with its own mode, producing same-length aligned columns in input order.
    #[test]
    fn eg098_align_multirate_common_grid() {
        // A: fast linear-interpolable @ 0,2,4 ; B: slow held @ 0 only.
        let a = StreamSpec::new(
            "imu",
            scalars(&[(0, 0.0), (2 * NS, 20.0), (4 * NS, 40.0)]),
            InterpMode::Linear,
            None,
        );
        let b = StreamSpec::new(
            "mode",
            scalars(&[(0, 1.0)]),
            InterpMode::AsofHold,
            None,
        );
        let grid = uniform_grid(0, 5 * NS, NS); // 0,1,2,3,4 s
        let frame = align_multirate(&[a, b], &grid);
        assert_eq!(frame.grid.len(), 5);
        assert_eq!(frame.channels.len(), 2);
        assert_eq!(frame.channels[0].name, "imu");
        // imu linear @ 0,10,20,30,40
        assert_eq!(
            frame.channels[0].values,
            vec![Some(0.0), Some(10.0), Some(20.0), Some(30.0), Some(40.0)]
        );
        // mode held @ 1 across the whole grid.
        assert_eq!(frame.channels[1].values, vec![Some(1.0); 5]);
    }

    /// EG-098: tumbling window starts align to width (EG-067 `(t/width)*width`).
    #[test]
    fn eg098_tumbling_window_starts_aligned() {
        // span [3,25) width 10 → starts 0,10,20.
        assert_eq!(tumbling_window_starts(3, 25, 10), vec![0, 10, 20]);
        assert_eq!(tumbling_window_starts(0, 10, 10), vec![0]);
        assert!(tumbling_window_starts(5, 5, 10).is_empty()); // empty span
        assert!(tumbling_window_starts(0, 10, 0).is_empty()); // bad width
    }

    /// EG-098: uniform grid is half-open [from,to) by step; guards bad step.
    #[test]
    fn eg098_uniform_grid_half_open() {
        assert_eq!(uniform_grid(0, 10, 3), vec![0, 3, 6, 9]);
        assert!(uniform_grid(0, 10, 0).is_empty());
        assert!(uniform_grid(10, 0, 3).is_empty());
    }

    /// EG-098: empty streams degrade to all-gap columns, never panic.
    #[test]
    fn eg098_empty_streams_degrade() {
        assert_eq!(resample(&[], &[0, 1, 2], InterpMode::Linear, None), vec![None; 3]);
        assert_eq!(resample(&[], &[0, 1, 2], InterpMode::Nearest, None), vec![None; 3]);
        assert_eq!(resample(&[], &[0, 1, 2], InterpMode::AsofHold, None), vec![None; 3]);
        let empty = StreamSpec::new("x", vec![], InterpMode::Nearest, None);
        let frame = align_multirate(&[empty], &[0, 1]);
        assert_eq!(frame.channels[0].values, vec![None, None]);
    }
}
