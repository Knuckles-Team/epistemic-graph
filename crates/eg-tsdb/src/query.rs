//! Native TS query primitives over a `Vec<Point>` (CONCEPT:KG-2.210).
//!
//! These are the operators a time-series workload needs that plain SQL / the graph
//! algebra do NOT give natively. They take the ts-sorted `Vec<Point>` the store
//! returns and need NOTHING but eg-core/eg-compute — NO DataFusion, so they work in
//! the lean / Pi (no-DataFusion) build.
//!
//!   * `time_bucket` windowed aggregate (avg/min/max/sum/count/first/last). Easy in
//!     DataFusion via `GROUP BY (ts/w)*w` (see `arrow_seg`); the native form here is
//!     the Pi path.
//!   * `asof_join_backward` — join a series to events/another series by NEAREST-in-
//!     time. THE critical primitive (ticks ↔ memories ↔ events); DataFusion 43 has
//!     no ASOF JOIN, so it MUST be native. O(L+R) merge over two ts-sorted inputs.
//!   * `gap_fill_locf` — emit a row on a fixed grid carrying the last observation
//!     forward across gaps. Also not native to DataFusion.
//!   * `ohlc_bars` / `downsample` — OHLC bars + rollup (continuous-aggregate building
//!     block).
//!   * `decay_weighted_mean` — recency-weighted series aggregate using the ONE shared
//!     Ebbinghaus curve (`eg_core::decay`) the memory layer uses (CONCEPT:KG-2.211).
//!
//! Where the math is already in the engine we REUSE it (`ewma_signal`,
//! `rolling_zscore` from `eg_compute::finance::signals`) — no re-implementation.

use crate::point::{Point, Ts};

/// The windowed-aggregate functions `time_bucket`/`downsample` support.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Agg {
    First,
    Last,
    Min,
    Max,
    Mean,
    Sum,
    Count,
}

/// One aggregated bucket: its aligned start, the aggregate value, and the count.
#[derive(Clone, Debug, PartialEq)]
pub struct Bucket {
    pub bucket_start: Ts,
    pub value: f64,
    pub count: usize,
}

/// `time_bucket(width, agg)` over field 0 of a ts-sorted series — the windowed
/// aggregate. Buckets are aligned to `width` (`(ts/width)*width`). Empty buckets are
/// omitted (use `gap_fill_locf` afterwards to densify). `width <= 0` ⇒ empty.
pub fn time_bucket(points: &[Point], width: Ts, agg: Agg) -> Vec<Bucket> {
    let mut out: Vec<Bucket> = Vec::new();
    if width <= 0 {
        return out;
    }
    let mut i = 0;
    while i < points.len() {
        let b = (points[i].ts / width) * width;
        let mut j = i;
        let mut acc: Vec<f64> = Vec::new();
        while j < points.len() && (points[j].ts / width) * width == b {
            acc.push(points[j].values[0]);
            j += 1;
        }
        out.push(Bucket {
            bucket_start: b,
            value: aggregate(&acc, agg),
            count: acc.len(),
        });
        i = j;
    }
    out
}

/// One OHLC bar: bucket start + open/high/low/close (field 0 = price) and volume
/// (field 1 if present, else 0).
#[derive(Clone, Debug, PartialEq)]
pub struct Ohlc {
    pub bucket_start: Ts,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

/// OHLC bars over a ts-sorted series. `width <= 0` ⇒ empty.
pub fn ohlc_bars(points: &[Point], width: Ts) -> Vec<Ohlc> {
    let mut out = Vec::new();
    if width <= 0 {
        return out;
    }
    let mut i = 0;
    while i < points.len() {
        let b = (points[i].ts / width) * width;
        let mut j = i;
        let p0 = points[i].values[0];
        let (open, mut high, mut low, mut close, mut vol) = (p0, p0, p0, p0, 0.0);
        while j < points.len() && (points[j].ts / width) * width == b {
            let p = points[j].values[0];
            high = high.max(p);
            low = low.min(p);
            close = p;
            if points[j].values.len() > 1 {
                vol += points[j].values[1];
            }
            j += 1;
        }
        out.push(Ohlc {
            bucket_start: b,
            open,
            high,
            low,
            close,
            volume: vol,
        });
        i = j;
    }
    out
}

fn aggregate(xs: &[f64], agg: Agg) -> f64 {
    match agg {
        Agg::First => xs.first().copied().unwrap_or(f64::NAN),
        Agg::Last => xs.last().copied().unwrap_or(f64::NAN),
        Agg::Min => xs.iter().copied().fold(f64::INFINITY, f64::min),
        Agg::Max => xs.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        Agg::Mean => {
            if xs.is_empty() {
                f64::NAN
            } else {
                xs.iter().sum::<f64>() / xs.len() as f64
            }
        }
        Agg::Sum => xs.iter().sum(),
        Agg::Count => xs.len() as f64,
    }
}

/// One ASOF output row: the left event time/value and the nearest prior right value.
#[derive(Clone, Debug, PartialEq)]
pub struct AsofRow {
    pub ts: Ts,
    pub left: f64,
    pub right: Option<f64>,
    pub right_ts: Option<Ts>,
}

/// ASOF join — for each `left` row find the `right` point with the GREATEST ts that
/// is `<= left.ts` (backward asof: "the state as of this event"), within `tolerance`
/// ns (`None` = unbounded). One output per left row. O(L + R) merge — both inputs are
/// already ts-sorted out of the store.
///
/// THE cross-series / series↔event primitive: "the price at the instant this memory
/// was written", "the regime in force when this trade fired".
pub fn asof_join_backward(left: &[Point], right: &[Point], tolerance: Option<i64>) -> Vec<AsofRow> {
    let mut out = Vec::with_capacity(left.len());
    let mut r = 0usize;
    let mut last: Option<(Ts, f64)> = None;
    for lp in left {
        while r < right.len() && right[r].ts <= lp.ts {
            last = Some((right[r].ts, right[r].values[0]));
            r += 1;
        }
        let matched = last.filter(|(rts, _)| match tolerance {
            Some(tol) => lp.ts - rts <= tol,
            None => true,
        });
        out.push(AsofRow {
            ts: lp.ts,
            left: lp.values[0],
            right: matched.map(|(_, v)| v),
            right_ts: matched.map(|(t, _)| t),
        });
    }
    out
}

/// One gap-fill grid row: grid ts, the LOCF value (`None` before the first obs), and
/// whether the value was carried forward (vs a real obs at this exact grid ts).
#[derive(Clone, Debug, PartialEq)]
pub struct GridRow {
    pub ts: Ts,
    pub value: Option<f64>,
    pub filled: bool,
}

/// Gap-fill on a fixed grid with last-observation-carried-forward (LOCF). Emits a row
/// at every `step` from `from` (inclusive) to `to` (exclusive); each grid point takes
/// the most recent observation at-or-before it. Grid points before the first obs are
/// `None`. `step <= 0` ⇒ empty.
pub fn gap_fill_locf(points: &[Point], from: Ts, to: Ts, step: Ts) -> Vec<GridRow> {
    let mut out = Vec::new();
    if step <= 0 {
        return out;
    }
    let mut i = 0usize;
    let mut last: Option<f64> = None;
    let mut t = from;
    while t < to {
        let mut real_here = false;
        while i < points.len() && points[i].ts <= t {
            last = Some(points[i].values[0]);
            if points[i].ts == t {
                real_here = true;
            }
            i += 1;
        }
        out.push(GridRow {
            ts: t,
            value: last,
            filled: last.is_some() && !real_here,
        });
        t += step;
    }
    out
}

/// Downsample / rollup: `time_bucket(Mean-style agg)` kept as `Vec<Point>` so the
/// result feeds straight back into the store (continuous-aggregate materialization)
/// or another primitive.
pub fn downsample(points: &[Point], width: Ts, agg: Agg) -> Vec<Point> {
    time_bucket(points, width, agg)
        .into_iter()
        .map(|b| Point::single(b.bucket_start, b.value))
        .collect()
}

// ───────────────────────── decay / recency composition ─────────────────────────

/// Recency-weighted mean of field 0 of a series as of `now` (ns), using the ONE
/// shared Ebbinghaus curve (`eg_core::decay`, CONCEPT:KG-2.211) — the SAME function
/// the semantic-memory confidence decay calls. `half_life_secs` parameterises the
/// curve; ages are computed in seconds (ns→s) so memory (days) and series (seconds)
/// are the same model at different scales. This is the "unify memory + series" proof:
/// time becomes the WEIGHT on a series aggregate.
pub fn decay_weighted_mean(points: &[Point], now: Ts, half_life_secs: f64) -> f64 {
    let mut wsum = 0.0;
    let mut vsum = 0.0;
    for p in points {
        let age_secs = (now - p.ts) as f64 / 1e9;
        let w = eg_core::decay::ebbinghaus_weight(age_secs, half_life_secs);
        wsum += w;
        vsum += w * p.values[0];
    }
    if wsum > 0.0 {
        vsum / wsum
    } else {
        f64::NAN
    }
}

// ───────────────────── finance-kernel reuse (no re-implementation) ─────────────

/// EWMA over field 0 of a stored series — delegates to the engine's `eg_compute`
/// signal kernel, proving the finance compute composes with stored series.
pub fn series_ewma(points: &[Point], span: usize) -> Vec<f64> {
    let vals: Vec<f64> = points.iter().map(|p| p.values[0]).collect();
    eg_compute::finance::signals::ewma_signal(&vals, span)
}

/// Rolling z-score over field 0 of a stored series — delegates to `eg_compute`.
pub fn series_rolling_zscore(points: &[Point], window: usize) -> Vec<f64> {
    let vals: Vec<f64> = points.iter().map(|p| p.values[0]).collect();
    eg_compute::finance::signals::rolling_zscore(&vals, window)
}
