//! Fuse a time-aligned multi-channel frame into a dense tensor + validity mask
//! (CONCEPT:EG-KG.query.multi-rate-sensor-stream) — the MODALITY half of robotics/IoT sensor fusion.
//!
//! The time-alignment half (multi-rate resample onto a common grid with per-channel
//! Nearest/Linear/AsofHold, and EG-067 tumbling windows) lives in [`eg_tsdb::fusion`];
//! this module stacks the resulting [`AlignedFrame`] into a `[timesteps × channels]`
//! eg-tensor [`Tensor`] frame (the fused multimodal frame for downstream ML) plus a
//! same-shape validity mask, encoding a per-cell GAP (`None`) as `NaN` in the `F64` frame
//! and `0` in the `U8` mask. A windowed variant reuses EG-067 tumbling-window semantics
//! to emit one fused tensor PER WINDOW, deterministically.
//!
//! Composes EG-085 (the [`Tensor`] value model) + eg-tsdb ASOF/windowing (EG-067).

use eg_tsdb::fusion::{align_multirate, uniform_grid, AlignedFrame, StreamSpec};
use eg_tsdb::point::Ts;

use crate::tensor::{Buffer, Tensor};

// Re-export the eg-tsdb alignment surface so callers can drive the whole fusion pipeline
// (build streams → align/window → tensor) from `eg_tensor::fusion` alone.
pub use eg_tsdb::fusion::{InterpMode, StreamSpec as Stream};

/// A fused multimodal tensor frame (CONCEPT:EG-KG.query.multi-rate-sensor-stream): a `[timesteps × channels]` `F64`
/// `frame` tensor (`NaN` at gaps) + a same-shape `U8` `mask` tensor (`1` = valid /
/// `0` = missing), plus the `grid` timestamps and channel `names` for provenance. The
/// two tensors share the exact shape `[grid.len(), names.len()]`.
#[derive(Clone, Debug, PartialEq)]
pub struct FusedFrame {
    /// `[T × C]` F64 sample values, `NaN` where a channel had no sample (a gap).
    pub frame: Tensor,
    /// `[T × C]` U8 validity mask, `1` where `frame` is a real sample, `0` at a gap.
    pub mask: Tensor,
    /// The common time base (row axis), length `T`.
    pub grid: Vec<Ts>,
    /// Channel names (column axis), length `C`, in input-stream order.
    pub names: Vec<String>,
}

impl FusedFrame {
    /// `(timesteps, channels)` — the shared shape of `frame` and `mask`.
    pub fn dims(&self) -> (usize, usize) {
        (self.grid.len(), self.names.len())
    }
}

/// Stack a time-[`AlignedFrame`] into a fused `[timesteps × channels]` tensor + mask
/// (CONCEPT:EG-KG.query.multi-rate-sensor-stream). Row `t` is the fusion instant `grid[t]`; column `c` is stream `c`.
/// A channel GAP (`None`) becomes `NaN` in `frame` and `0` in `mask`. Errors only if a
/// channel column length disagrees with the grid (a malformed [`AlignedFrame`]).
pub fn fuse_aligned(aligned: &AlignedFrame) -> Result<FusedFrame, String> {
    let t = aligned.grid.len();
    let c = aligned.channels.len();
    for ch in &aligned.channels {
        if ch.values.len() != t {
            return Err(format!(
                "EG-098 fuse: channel '{}' has {} values but grid has {}",
                ch.name,
                ch.values.len(),
                t
            ));
        }
    }
    let mut frame = Vec::with_capacity(t * c);
    let mut mask = Vec::with_capacity(t * c);
    // Row-major: for each timestep, walk the channels (C-order, last axis fastest).
    for ti in 0..t {
        for ch in &aligned.channels {
            match ch.values[ti] {
                Some(v) => {
                    frame.push(v);
                    mask.push(1u8);
                }
                None => {
                    frame.push(f64::NAN);
                    mask.push(0u8);
                }
            }
        }
    }
    Ok(FusedFrame {
        frame: Tensor::new(vec![t, c], Buffer::F64(frame))?,
        mask: Tensor::new(vec![t, c], Buffer::U8(mask))?,
        grid: aligned.grid.clone(),
        names: aligned.channels.iter().map(|ch| ch.name.clone()).collect(),
    })
}

/// Align N multi-rate `streams` onto `grid` and fuse them into one [`FusedFrame`]
/// (CONCEPT:EG-KG.query.multi-rate-sensor-stream) — the one-shot alignment + tensor-stack over a caller-supplied grid.
pub fn fuse_on_grid(streams: &[StreamSpec], grid: &[Ts]) -> Result<FusedFrame, String> {
    fuse_aligned(&align_multirate(streams, grid))
}

/// One tumbling window's fused output (CONCEPT:EG-KG.query.multi-rate-sensor-stream): the window START ts + its fused
/// tensor frame.
#[derive(Clone, Debug, PartialEq)]
pub struct WindowFrame {
    pub window_start: Ts,
    pub frame: FusedFrame,
}

/// Windowed multimodal fusion (CONCEPT:EG-KG.query.multi-rate-sensor-stream): partition the time axis into TUMBLING
/// windows of `width` (EG-067 semantics, aligned as `(t/width)*width`) spanning the union
/// sample span; within each window resample every stream onto a uniform `step`-spaced
/// sub-grid and fuse it into a `[timesteps × channels]` tensor. Emits one [`WindowFrame`]
/// per window across the span, in ASCENDING window order — deterministic. `width <= 0`
/// or `step <= 0` ⇒ empty; streams with no samples ⇒ empty. Assumes non-negative
/// ts-sorted samples (epoch-ns), consistent with `time_bucket`.
pub fn windowed_fusion(
    streams: &[StreamSpec],
    width: Ts,
    step: Ts,
) -> Result<Vec<WindowFrame>, String> {
    if width <= 0 || step <= 0 {
        return Ok(Vec::new());
    }
    // Union sample span from the ts-sorted streams (first = min ts, last = max ts).
    let mut min_ts = Ts::MAX;
    let mut max_ts = Ts::MIN;
    for s in streams {
        if let Some(f) = s.points.first() {
            min_ts = min_ts.min(f.ts);
        }
        if let Some(l) = s.points.last() {
            max_ts = max_ts.max(l.ts);
        }
    }
    if min_ts > max_ts {
        return Ok(Vec::new()); // no samples anywhere
    }
    let mut out = Vec::new();
    let mut ws = (min_ts / width) * width; // first window start (EG-067 alignment)
    let last = (max_ts / width) * width; // last window that can contain a sample
    while ws <= last {
        let grid = uniform_grid(ws, ws + width, step);
        if !grid.is_empty() {
            out.push(WindowFrame {
                window_start: ws,
                frame: fuse_on_grid(streams, &grid)?,
            });
        }
        ws += width;
    }
    Ok(out)
}

#[cfg(test)]
mod eg098_fusion_tests {
    use super::*;
    use eg_tsdb::point::Point;

    const NS: i64 = 1_000_000_000;

    fn scalars(pairs: &[(i64, f64)]) -> Vec<Point> {
        pairs.iter().map(|&(t, v)| Point::single(t, v)).collect()
    }

    /// EG-098: the fused frame is a [timesteps × channels] tensor; frame & mask share
    /// that shape.
    #[test]
    fn eg098_fuse_aligned_frame_shape_timesteps_by_channels() {
        let a = StreamSpec::new(
            "imu",
            scalars(&[(0, 0.0), (2 * NS, 20.0), (4 * NS, 40.0)]),
            InterpMode::Linear,
            None,
        );
        let b = StreamSpec::new("gps", scalars(&[(0, 100.0)]), InterpMode::AsofHold, None);
        let grid = uniform_grid(0, 5 * NS, NS); // 5 timesteps
        let fused = fuse_on_grid(&[a, b], &grid).unwrap();

        assert_eq!(fused.dims(), (5, 2));
        assert_eq!(fused.frame.shape, vec![5, 2]);
        assert_eq!(fused.mask.shape, vec![5, 2]);
        assert_eq!(fused.frame.dtype, crate::DType::F64);
        assert_eq!(fused.mask.dtype, crate::DType::U8);
        assert_eq!(fused.names, vec!["imu".to_string(), "gps".to_string()]);

        // Row-major [T,C]: cell (t=2, imu) = linear @ 2s = 20; (t=2, gps) held = 100.
        if let Buffer::F64(v) = &fused.frame.data {
            assert_eq!(v[2 * 2], 20.0); // (t=2, c=0)
            assert_eq!(v[2 * 2 + 1], 100.0); // (t=2, c=1)
        } else {
            panic!("frame must be F64");
        }
    }

    /// EG-098: a channel GAP is encoded as NaN in the frame and 0 in the validity mask;
    /// a real sample is a finite value and mask 1.
    #[test]
    fn eg098_fuse_encodes_gap_as_nan_and_mask_zero() {
        // gps has ONE sample @ 0 with a 1s AsofHold tolerance → stale (gap) by t=2s.
        let a = StreamSpec::new(
            "imu",
            scalars(&[(0, 10.0), (NS, 20.0), (2 * NS, 30.0)]),
            InterpMode::Nearest,
            None,
        );
        let b = StreamSpec::new(
            "gps",
            scalars(&[(0, 100.0)]),
            InterpMode::AsofHold,
            Some(NS),
        );
        let grid = vec![0, NS, 2 * NS];
        let fused = fuse_on_grid(&[a, b], &grid).unwrap();

        let frame = match &fused.frame.data {
            Buffer::F64(v) => v,
            _ => panic!("F64"),
        };
        let mask = match &fused.mask.data {
            Buffer::U8(v) => v,
            _ => panic!("U8"),
        };
        // Layout [3×2]: rows = t0,t1,t2 ; cols = imu,gps.
        // imu always valid; gps valid @ t0,t1 (held within 1s), GAP @ t2 (2s stale).
        assert_eq!(mask, &vec![1, 1, 1, 1, 1, 0]);
        assert!(frame[5].is_nan()); // (t=2, gps) gap → NaN
        assert_eq!(frame[4], 30.0); // (t=2, imu) real
        assert!(frame[..5].iter().all(|x| x.is_finite()));
    }

    /// EG-098: windowed fusion emits one fused tensor PER tumbling window (EG-067), each
    /// [timesteps × channels], covering the sample span.
    #[test]
    fn eg098_windowed_fusion_one_tensor_per_window() {
        // 10s of a 1-per-second scalar; window = 4s, sub-grid step = 1s.
        let a = StreamSpec::new(
            "imu",
            (0..10).map(|i| Point::single(i * NS, i as f64)).collect(),
            InterpMode::AsofHold,
            None,
        );
        let windows = windowed_fusion(&[a], 4 * NS, NS).unwrap();

        // span [0,9s] → window starts 0,4,8 (aligned to 4s).
        assert_eq!(
            windows.iter().map(|w| w.window_start).collect::<Vec<_>>(),
            vec![0, 4 * NS, 8 * NS]
        );
        // Each of the first two windows has 4 timesteps × 1 channel.
        assert_eq!(windows[0].frame.dims(), (4, 1));
        assert_eq!(windows[1].frame.dims(), (4, 1));
        // Window @ 4s sub-grid 4,5,6,7 → held values 4,5,6,7.
        if let Buffer::F64(v) = &windows[1].frame.frame.data {
            assert_eq!(v, &vec![4.0, 5.0, 6.0, 7.0]);
        } else {
            panic!("F64");
        }
    }

    /// EG-098: windowed fusion is deterministic — identical input yields identical output.
    #[test]
    fn eg098_windowed_fusion_deterministic() {
        let mk = || {
            vec![
                StreamSpec::new(
                    "imu",
                    (0..6).map(|i| Point::single(i * NS, i as f64)).collect(),
                    InterpMode::Linear,
                    None,
                ),
                StreamSpec::new(
                    "gps",
                    scalars(&[(0, 100.0), (3 * NS, 130.0)]),
                    InterpMode::AsofHold,
                    None,
                ),
            ]
        };
        let a = windowed_fusion(&mk(), 3 * NS, NS).unwrap();
        let b = windowed_fusion(&mk(), 3 * NS, NS).unwrap();
        assert_eq!(a, b);
        // 2 channels per window.
        assert!(a.iter().all(|w| w.frame.dims().1 == 2));
    }

    /// EG-098: empty / degenerate input yields an empty windowed result and an empty
    /// fused frame (shape [0, C]) — degrade, never panic.
    #[test]
    fn eg098_empty_input_yields_empty_frame() {
        assert!(windowed_fusion(&[], 4 * NS, NS).unwrap().is_empty());
        // bad width/step guarded.
        let a = StreamSpec::new("x", scalars(&[(0, 1.0)]), InterpMode::Nearest, None);
        assert!(windowed_fusion(std::slice::from_ref(&a), 0, NS)
            .unwrap()
            .is_empty());
        assert!(windowed_fusion(std::slice::from_ref(&a), NS, 0)
            .unwrap()
            .is_empty());
        // Empty grid → [0 × 1] frame.
        let fused = fuse_on_grid(std::slice::from_ref(&a), &[]).unwrap();
        assert_eq!(fused.dims(), (0, 1));
        assert_eq!(fused.frame.numel(), 0);
    }

    /// EG-098: fuse_aligned rejects a malformed AlignedFrame (channel length ≠ grid).
    #[test]
    fn eg098_fuse_rejects_ragged_frame() {
        use eg_tsdb::fusion::{AlignedChannel, AlignedFrame};
        let bad = AlignedFrame {
            grid: vec![0, 1, 2],
            channels: vec![AlignedChannel {
                name: "x".into(),
                values: vec![Some(1.0)], // len 1 ≠ grid 3
            }],
        };
        assert!(fuse_aligned(&bad).is_err());
    }
}
