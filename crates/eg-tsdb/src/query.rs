//! Native TS query primitives over a `Vec<Point>` (CONCEPT:AU-KG.retrieval.god-nodes-communities).
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
//!     time. THE critical primitive (ticks ↔ memories ↔ events); DataFusion 54 has
//!     no ASOF JOIN, so it MUST be native. O(L+R) merge over two ts-sorted inputs.
//!   * `gap_fill_locf` — emit a row on a fixed grid carrying the last observation
//!     forward across gaps. Also not native to DataFusion.
//!   * `ohlc_bars` / `downsample` — OHLC bars + rollup (continuous-aggregate building
//!     block).
//!   * `decay_weighted_mean` — recency-weighted series aggregate using the ONE shared
//!     Ebbinghaus curve (`eg_core::decay`) the memory layer uses (CONCEPT:EG-KG.compute.handled-outside-single-anchor).
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
        let mut count = 0usize;
        let mut value = match agg {
            Agg::Min => f64::INFINITY,
            Agg::Max => f64::NEG_INFINITY,
            _ => 0.0,
        };
        while j < points.len() && (points[j].ts / width) * width == b {
            let sample = points[j].values[0];
            match agg {
                Agg::First if count == 0 => value = sample,
                Agg::First => {}
                Agg::Last => value = sample,
                Agg::Min => value = value.min(sample),
                Agg::Max => value = value.max(sample),
                Agg::Mean | Agg::Sum => value += sample,
                Agg::Count => {}
            }
            count += 1;
            j += 1;
        }
        if agg == Agg::Mean {
            value /= count as f64;
        } else if agg == Agg::Count {
            value = count as f64;
        }
        out.push(Bucket {
            bucket_start: b,
            value,
            count,
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
/// shared Ebbinghaus curve (`eg_core::decay`, CONCEPT:EG-KG.compute.handled-outside-single-anchor) — the SAME function
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

// ───────────────────── multimodal sensor fusion (CONCEPT:EG-KG.query.multi-rate-sensor-stream) ────────────────
//
// Time-align N HETEROGENEOUS sensor streams (camera/LiDAR/audio/tactile/proprioceptive)
// to ONE common clock and emit fused multi-channel rows. Each stream's cell is an OPAQUE
// value carried through untouched: a plain scalar reading OR a reference to an EG-085
// tensor blob (a camera/LiDAR frame). The alignment REUSES `asof_join_backward` — for
// each reference-clock instant, each stream contributes its latest sample at-or-before
// that instant, within `tolerance` ns; a stream too stale (or with no prior sample)
// yields a GAP (`None`) for that channel. This composes the eg-tsdb ASOF primitive
// (EG-067) with the EG-085 tensor modality into the robotics fusion op EG-088's CEP can
// then run over.

/// One opaque sample cell in a sensor stream (CONCEPT:EG-KG.query.multi-rate-sensor-stream). Either a plain scalar
/// reading, or an opaque BLOB reference — an EG-085 tensor blob id (a camera/LiDAR
/// frame) — carried through fusion UNTOUCHED. `sensor_fuse` never decodes a `Blob`
/// (per-channel tensor decode is a documented follow-up); it only time-aligns the cell.
#[derive(Clone, Debug, PartialEq)]
pub enum Cell {
    /// A scalar sensor reading.
    Scalar(f64),
    /// An opaque reference (e.g. an EG-085 tensor blob id) carried through as-is.
    Blob(String),
}

/// One timestamped sample in a sensor stream: a ts (ns) + an opaque [`Cell`].
#[derive(Clone, Debug, PartialEq)]
pub struct Sample {
    pub ts: Ts,
    pub cell: Cell,
}

impl Sample {
    /// A scalar sample at `ts`.
    pub fn scalar(ts: Ts, value: f64) -> Self {
        Self {
            ts,
            cell: Cell::Scalar(value),
        }
    }

    /// A blob-reference sample at `ts` (e.g. an EG-085 tensor frame id).
    pub fn blob(ts: Ts, reference: impl Into<String>) -> Self {
        Self {
            ts,
            cell: Cell::Blob(reference.into()),
        }
    }
}

/// A named heterogeneous sensor stream to fuse (CONCEPT:EG-KG.query.multi-rate-sensor-stream). `samples` MUST be
/// ts-sorted (as they come out of the store) — the ASOF merge relies on it.
#[derive(Clone, Debug, PartialEq)]
pub struct SeriesRef {
    /// The stream / channel name (for identifying which column is which).
    pub name: String,
    /// The stream's ts-sorted samples.
    pub samples: Vec<Sample>,
}

/// One fused multi-channel row (CONCEPT:EG-KG.query.multi-rate-sensor-stream): the common-clock timestamp and one
/// aligned cell per input stream, IN STREAM ORDER. A `None` channel is a GAP — that
/// stream had no sample at-or-before `ts` within `tolerance`.
#[derive(Clone, Debug, PartialEq)]
pub struct FusedRow {
    pub ts: Ts,
    pub channels: Vec<Option<Cell>>,
}

/// Time-align N heterogeneous sensor `streams` to ONE common clock and emit fused
/// multi-channel rows (CONCEPT:EG-KG.query.multi-rate-sensor-stream). The common clock is the sorted, de-duplicated
/// UNION of every stream's sample timestamps — so every sensor event across all streams
/// becomes a fusion instant. For each instant, each stream contributes its latest sample
/// at-or-before that instant via a backward ASOF join (`asof_join_backward`), within
/// `tolerance` ns (`None` = unbounded); a stream with no prior sample, or one too stale,
/// yields a GAP (`None`) for that channel. A stream's value is carried through as an
/// opaque [`Cell`] — a scalar OR an EG-085 tensor-blob reference (camera/LiDAR frame) —
/// so fusion is modality-agnostic.
///
/// The implementation k-way merges the already-sorted clocks, then advances one
/// monotonic backward-ASOF cursor per stream. This applies the same matching rule as
/// [`asof_join_backward`] without allocating index-encoded point copies or a
/// transposed intermediate channel matrix. Streams must be ts-sorted; empty input
/// (or all-empty streams) yields no rows.
pub fn sensor_fuse(streams: &[SeriesRef], tolerance: Option<i64>) -> Vec<FusedRow> {
    // 1. Merge the already-sorted input clocks. A binary heap keeps one frontier
    // item per stream, reducing union construction from O(T log T) over a flattened
    // copy to O(T log S), where T is total samples and S is stream count.
    let clock = merged_clock(streams);
    if clock.is_empty() {
        return Vec::new();
    }

    // 2. Advance one monotonic cursor per stream while walking the common clock.
    // This is the same backward-ASOF rule as `asof_join_backward`, but writes each
    // final row directly. It avoids index-encoded Point copies plus the O(S*R)
    // intermediate column matrix that was transposed into the returned O(S*R)
    // rows (R = union-clock length).
    let mut cursors = vec![0usize; streams.len()];
    let mut rows = Vec::with_capacity(clock.len());
    for ts in clock {
        let mut channels = Vec::with_capacity(streams.len());
        for (stream_index, stream) in streams.iter().enumerate() {
            let cursor = &mut cursors[stream_index];
            while *cursor < stream.samples.len() && stream.samples[*cursor].ts <= ts {
                *cursor += 1;
            }
            let cell = cursor.checked_sub(1).and_then(|sample_index| {
                let sample = &stream.samples[sample_index];
                let within_tolerance = tolerance
                    .map(|limit| ts.saturating_sub(sample.ts) <= limit)
                    .unwrap_or(true);
                within_tolerance.then(|| sample.cell.clone())
            });
            channels.push(cell);
        }
        rows.push(FusedRow { ts, channels });
    }
    rows
}

/// Sorted, de-duplicated union of the sorted stream clocks using a k-way merge.
fn merged_clock(streams: &[SeriesRef]) -> Vec<Ts> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let total = streams.iter().map(|stream| stream.samples.len()).sum();
    let mut clock = Vec::with_capacity(total);
    let mut frontier: BinaryHeap<Reverse<(Ts, usize, usize)>> = BinaryHeap::new();
    for (stream_index, stream) in streams.iter().enumerate() {
        if let Some(first) = stream.samples.first() {
            frontier.push(Reverse((first.ts, stream_index, 0)));
        }
    }
    while let Some(Reverse((ts, stream_index, sample_index))) = frontier.pop() {
        if clock.last().copied() != Some(ts) {
            clock.push(ts);
        }
        let next = sample_index + 1;
        if let Some(sample) = streams[stream_index].samples.get(next) {
            frontier.push(Reverse((sample.ts, stream_index, next)));
        }
    }
    clock
}

#[cfg(test)]
mod sensor_fuse_tests {
    use super::*;

    const NS: i64 = 1_000_000_000; // 1 second in ns

    /// 3 streams at DIFFERENT rates fuse to aligned multi-channel rows on the union
    /// clock; each channel carries its latest sample at-or-before each instant, and a
    /// tensor-blob channel is carried through as an opaque cell.
    #[test]
    fn three_streams_different_rates_fuse_aligned() {
        // A: fast scalar @ 0,1,2,3,4 ; B: medium scalar @ 0,2,4 ; C: slow LiDAR blob @ 0.
        let a = SeriesRef {
            name: "imu".into(),
            samples: (0..5).map(|i| Sample::scalar(i * NS, i as f64)).collect(),
        };
        let b = SeriesRef {
            name: "gps".into(),
            samples: vec![
                Sample::scalar(0, 100.0),
                Sample::scalar(2 * NS, 120.0),
                Sample::scalar(4 * NS, 140.0),
            ],
        };
        let c = SeriesRef {
            name: "lidar".into(),
            samples: vec![Sample::blob(0, "blob://frame0")],
        };
        let fused = sensor_fuse(&[a, b, c], None);

        // Union clock = {0,1,2,3,4} → 5 fused rows, in order.
        assert_eq!(fused.len(), 5);
        assert_eq!(
            fused.iter().map(|r| r.ts).collect::<Vec<_>>(),
            vec![0, NS, 2 * NS, 3 * NS, 4 * NS]
        );
        // Every row carries all 3 channels.
        assert!(fused.iter().all(|r| r.channels.len() == 3));

        // @ ts=3: A latest = sample@3 (=3.0); B latest at-or-before 3 = sample@2 (=120);
        // C latest = the blob@0, carried through opaque.
        let r3 = &fused[3];
        assert_eq!(r3.channels[0], Some(Cell::Scalar(3.0)));
        assert_eq!(r3.channels[1], Some(Cell::Scalar(120.0)));
        assert_eq!(r3.channels[2], Some(Cell::Blob("blob://frame0".into())));

        // @ ts=4: A=4.0, B=140 (real sample@4), C still the blob@0 (unbounded tolerance).
        let r4 = &fused[4];
        assert_eq!(r4.channels[0], Some(Cell::Scalar(4.0)));
        assert_eq!(r4.channels[1], Some(Cell::Scalar(140.0)));
        assert_eq!(r4.channels[2], Some(Cell::Blob("blob://frame0".into())));
    }

    /// A sample OUTSIDE `tolerance` (or with no prior sample) yields a GAP (`None`).
    #[test]
    fn stale_or_missing_sample_yields_gap() {
        // A: scalar @ 0,2,4 ; C: a single blob @ 0. Tolerance = 1s.
        let a = SeriesRef {
            name: "imu".into(),
            samples: vec![
                Sample::scalar(0, 10.0),
                Sample::scalar(2 * NS, 20.0),
                Sample::scalar(4 * NS, 40.0),
            ],
        };
        let c = SeriesRef {
            name: "lidar".into(),
            samples: vec![Sample::blob(0, "blob://frame0")],
        };
        let fused = sensor_fuse(&[a, c], Some(NS));

        // Clock = {0,2,4}. C only has a sample@0, so at ts=2 (2s stale) and ts=4 it is a
        // GAP under a 1s tolerance; A is always fresh (exact hit each instant).
        assert_eq!(fused.len(), 3);
        assert_eq!(
            fused[0].channels,
            vec![
                Some(Cell::Scalar(10.0)),
                Some(Cell::Blob("blob://frame0".into()))
            ]
        );
        assert_eq!(fused[1].channels, vec![Some(Cell::Scalar(20.0)), None]);
        assert_eq!(fused[2].channels, vec![Some(Cell::Scalar(40.0)), None]);
    }

    /// Empty input (and all-empty streams) yields no rows — degrade, never panic.
    #[test]
    fn empty_input_yields_no_rows() {
        assert!(sensor_fuse(&[], None).is_empty());
        let empty = SeriesRef {
            name: "x".into(),
            samples: vec![],
        };
        assert!(sensor_fuse(&[empty], None).is_empty());
    }

    #[test]
    fn duplicate_clock_ticks_choose_the_last_sample_at_that_tick() {
        let a = SeriesRef {
            name: "a".into(),
            samples: vec![
                Sample::scalar(1, 10.0),
                Sample::scalar(1, 11.0),
                Sample::scalar(3, 30.0),
            ],
        };
        let b = SeriesRef {
            name: "b".into(),
            samples: vec![Sample::scalar(2, 20.0), Sample::scalar(3, 31.0)],
        };
        let fused = sensor_fuse(&[a, b], None);
        assert_eq!(
            fused.iter().map(|row| row.ts).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(fused[0].channels, vec![Some(Cell::Scalar(11.0)), None]);
        assert_eq!(
            fused[2].channels,
            vec![Some(Cell::Scalar(30.0)), Some(Cell::Scalar(31.0))]
        );
    }
}

#[cfg(test)]
mod aggregation_tests {
    use super::*;

    #[test]
    fn streaming_time_bucket_preserves_every_aggregate() {
        let points = vec![
            Point::single(0, 3.0),
            Point::single(1, 1.0),
            Point::single(2, 5.0),
            Point::single(10, 7.0),
        ];
        let first = time_bucket(&points, 10, Agg::First);
        let last = time_bucket(&points, 10, Agg::Last);
        let min = time_bucket(&points, 10, Agg::Min);
        let max = time_bucket(&points, 10, Agg::Max);
        let mean = time_bucket(&points, 10, Agg::Mean);
        let sum = time_bucket(&points, 10, Agg::Sum);
        let count = time_bucket(&points, 10, Agg::Count);

        assert_eq!(first[0].value, 3.0);
        assert_eq!(last[0].value, 5.0);
        assert_eq!(min[0].value, 1.0);
        assert_eq!(max[0].value, 5.0);
        assert_eq!(mean[0].value, 3.0);
        assert_eq!(sum[0].value, 9.0);
        assert_eq!(count[0].value, 3.0);
        assert_eq!(count[0].count, 3);
        assert_eq!(count[1].value, 1.0);
    }
}
