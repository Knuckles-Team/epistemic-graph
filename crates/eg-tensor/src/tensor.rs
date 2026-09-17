//! The dense N-D array value model (CONCEPT:EG-KG.storage.content-addressed-dedup): a [`Tensor`] = `dtype` + `shape`
//! + a row-major typed [`Buffer`]. Coordinates are C-order (last axis varies fastest).
//!
//! Every type derives serde so a `Tensor` persists as a typed value in the engine's
//! redb per-graph store; the compact byte-blob codec ([`Tensor::to_blob`] /
//! [`Tensor::from_blob`], in `crate::blob`) gives the content-addressable form for the
//! blob CAS. Ops are hand-written and pure-Rust — no BLAS/C — so the crate is Pi-safe.

use serde::{Deserialize, Serialize};

/// The element type of a [`Tensor`]. The `u8` discriminants are the on-wire tag in the
/// compact byte-blob header (`crate::blob`); keep them STABLE.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DType {
    F32,
    F64,
    I32,
    I64,
    U8,
}

impl DType {
    /// The stable header tag byte used by the compact blob codec.
    pub(crate) fn tag(self) -> u8 {
        match self {
            DType::F32 => 0,
            DType::F64 => 1,
            DType::I32 => 2,
            DType::I64 => 3,
            DType::U8 => 4,
        }
    }

    /// Inverse of [`DType::tag`].
    pub(crate) fn from_tag(t: u8) -> Option<DType> {
        Some(match t {
            0 => DType::F32,
            1 => DType::F64,
            2 => DType::I32,
            3 => DType::I64,
            4 => DType::U8,
            _ => return None,
        })
    }
}

/// A row-major typed data buffer. The variant IS the [`DType`]; every element of the
/// tensor lives here in C-order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Buffer {
    F32(Vec<f32>),
    F64(Vec<f64>),
    I32(Vec<i32>),
    I64(Vec<i64>),
    U8(Vec<u8>),
}

impl Buffer {
    /// The [`DType`] this buffer holds.
    pub fn dtype(&self) -> DType {
        match self {
            Buffer::F32(_) => DType::F32,
            Buffer::F64(_) => DType::F64,
            Buffer::I32(_) => DType::I32,
            Buffer::I64(_) => DType::I64,
            Buffer::U8(_) => DType::U8,
        }
    }

    /// Number of elements.
    pub fn len(&self) -> usize {
        match self {
            Buffer::F32(v) => v.len(),
            Buffer::F64(v) => v.len(),
            Buffer::I32(v) => v.len(),
            Buffer::I64(v) => v.len(),
            Buffer::U8(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Widen every element to `f64` (the common accumulator for reduce / elementwise).
    fn to_f64(&self) -> Vec<f64> {
        match self {
            Buffer::F32(v) => v.iter().map(|&x| x as f64).collect(),
            Buffer::F64(v) => v.clone(),
            Buffer::I32(v) => v.iter().map(|&x| x as f64).collect(),
            Buffer::I64(v) => v.iter().map(|&x| x as f64).collect(),
            Buffer::U8(v) => v.iter().map(|&x| x as f64).collect(),
        }
    }

    /// Narrow an `f64` slice back into a buffer of `dtype` (numeric casts truncate, as
    /// `as` does — integer mean therefore truncates toward zero).
    fn from_f64(dtype: DType, data: &[f64]) -> Buffer {
        match dtype {
            DType::F32 => Buffer::F32(data.iter().map(|&x| x as f32).collect()),
            DType::F64 => Buffer::F64(data.to_vec()),
            DType::I32 => Buffer::I32(data.iter().map(|&x| x as i32).collect()),
            DType::I64 => Buffer::I64(data.iter().map(|&x| x as i64).collect()),
            DType::U8 => Buffer::U8(data.iter().map(|&x| x as u8).collect()),
        }
    }
}

/// How [`Tensor::reduce`] collapses one axis.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReduceKind {
    Sum,
    Mean,
    Max,
    Min,
}

/// The scalar op [`Tensor::elementwise`] applies to every element.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ElementwiseOp {
    Add,
    Sub,
    Mul,
    Div,
}

/// A dense N-D array: a `dtype`/`shape` manifest over a row-major [`Buffer`]
/// (CONCEPT:EG-KG.storage.content-addressed-dedup). The invariant `shape.iter().product() == data.len()` holds for any
/// `Tensor` returned by this crate; [`Tensor::new`] enforces it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Tensor {
    pub dtype: DType,
    pub shape: Vec<usize>,
    pub data: Buffer,
}

impl Tensor {
    /// Build a tensor, validating that `data`'s length equals `shape`'s product and
    /// that `data`'s variant matches `dtype`.
    pub fn new(shape: Vec<usize>, data: Buffer) -> Result<Tensor, String> {
        let n: usize = shape.iter().product();
        if data.len() != n {
            return Err(format!(
                "tensor buffer len {} != shape product {} (shape {:?})",
                data.len(),
                n,
                shape
            ));
        }
        Ok(Tensor {
            dtype: data.dtype(),
            shape,
            data,
        })
    }

    /// Total element count (`shape` product).
    pub fn numel(&self) -> usize {
        self.data.len()
    }

    /// Number of dimensions.
    pub fn ndim(&self) -> usize {
        self.shape.len()
    }

    /// Row-major (C-order) strides for the current shape.
    fn strides(&self) -> Vec<usize> {
        strides(&self.shape)
    }

    /// Extract a hyper-rectangular sub-tensor. `ranges[d] = (start, end)` is a
    /// half-open `start..end` on axis `d`; there must be one range per axis. dtype- and
    /// byte-preserving (no numeric widening).
    pub fn slice(&self, ranges: &[(usize, usize)]) -> Result<Tensor, String> {
        if ranges.len() != self.ndim() {
            return Err(format!(
                "slice needs one range per axis: got {} ranges for {} dims",
                ranges.len(),
                self.ndim()
            ));
        }
        for (d, &(a, b)) in ranges.iter().enumerate() {
            if a > b || b > self.shape[d] {
                return Err(format!(
                    "slice range {:?} out of bounds on axis {} (dim {})",
                    (a, b),
                    d,
                    self.shape[d]
                ));
            }
        }
        let out_shape: Vec<usize> = ranges.iter().map(|&(a, b)| b - a).collect();
        let strides = self.strides();
        let data = match &self.data {
            Buffer::F32(v) => Buffer::F32(gather(v, &out_shape, ranges, &strides)),
            Buffer::F64(v) => Buffer::F64(gather(v, &out_shape, ranges, &strides)),
            Buffer::I32(v) => Buffer::I32(gather(v, &out_shape, ranges, &strides)),
            Buffer::I64(v) => Buffer::I64(gather(v, &out_shape, ranges, &strides)),
            Buffer::U8(v) => Buffer::U8(gather(v, &out_shape, ranges, &strides)),
        };
        Tensor::new(out_shape, data)
    }

    /// Reduce one `axis` with `kind`, dropping that axis from the shape (a rank-`n` →
    /// rank-`n-1` reduction). Accumulates in `f64`, then narrows back to `dtype`.
    pub fn reduce(&self, axis: usize, kind: ReduceKind) -> Result<Tensor, String> {
        if axis >= self.ndim() {
            return Err(format!(
                "reduce axis {} out of range for {} dims",
                axis,
                self.ndim()
            ));
        }
        let src = self.data.to_f64();
        let lane = ReduceLane {
            len: self.shape[axis],
            stride: self.strides()[axis],
            kind,
        };
        let out_shape: Vec<usize> = self
            .shape
            .iter()
            .enumerate()
            .filter(|&(d, _)| d != axis)
            .map(|(_, &s)| s)
            .collect();
        let out_n: usize = out_shape.iter().product();
        let out_strides = strides_including_removed(&self.shape, axis);

        let mut out = vec![0.0f64; out_n];
        // For each output cell, walk the `lane.len` elements along `axis`.
        let mut coord = vec![0usize; out_shape.len()];
        for slot in out.iter_mut() {
            // Base flat index in the SOURCE for this output coordinate (axis index 0).
            let base: usize = coord.iter().zip(&out_strides).map(|(&c, &s)| c * s).sum();
            *slot = lane.fold(&src, base);
            advance_c_order(&mut coord, &out_shape);
        }
        Tensor::new(out_shape, Buffer::from_f64(self.dtype, &out))
    }

    /// Apply a scalar op to every element (dtype-preserving; computed in `f64` then
    /// narrowed). Shape is unchanged.
    pub fn elementwise(&self, op: ElementwiseOp, scalar: f64) -> Tensor {
        // Apply the scalar op on the ACTIVE tensor backend (CONCEPT:EG-KG.compute.gpu-distance-seam): the CUDA
        // kernel when `gpu-cuda` is built + a device is present, else the pure-Rust CPU
        // map — identical results either way.
        let out: Vec<f64> = crate::gpu::elementwise_dispatch(&self.data.to_f64(), op, scalar);
        Tensor {
            dtype: self.dtype,
            shape: self.shape.clone(),
            data: Buffer::from_f64(self.dtype, &out),
        }
    }

    /// Re-interpret the same row-major buffer under a new `shape` (same element count).
    /// Zero-copy in spirit (the buffer is moved), so it is dtype- and byte-preserving.
    pub fn reshape(&self, new_shape: Vec<usize>) -> Result<Tensor, String> {
        let n: usize = new_shape.iter().product();
        if n != self.numel() {
            return Err(format!(
                "reshape {:?} ({} elems) incompatible with {} elems",
                new_shape,
                n,
                self.numel()
            ));
        }
        Ok(Tensor {
            dtype: self.dtype,
            shape: new_shape,
            data: self.data.clone(),
        })
    }
}

/// Row-major (C-order) strides for `shape`.
fn strides(shape: &[usize]) -> Vec<usize> {
    let mut s = vec![1usize; shape.len()];
    for i in (0..shape.len().saturating_sub(1)).rev() {
        s[i] = s[i + 1] * shape[i + 1];
    }
    s
}

/// The run of source elements one output cell of [`Tensor::reduce`] folds: `len`
/// elements `stride` apart along the reduced axis, combined with `kind`.
struct ReduceLane {
    len: usize,
    stride: usize,
    kind: ReduceKind,
}

impl ReduceLane {
    /// Fold the lane starting at flat source index `base` (a mean over an empty lane
    /// stays at the additive identity).
    fn fold(&self, src: &[f64], base: usize) -> f64 {
        let acc = (0..self.len)
            .map(|a| src[base + a * self.stride])
            .fold(reduce_identity(self.kind), |acc, x| {
                reduce_step(self.kind, acc, x)
            });
        match self.kind {
            ReduceKind::Mean if self.len > 0 => acc / self.len as f64,
            ReduceKind::Sum | ReduceKind::Mean | ReduceKind::Max | ReduceKind::Min => acc,
        }
    }
}

/// The accumulator's starting value for `kind`.
fn reduce_identity(kind: ReduceKind) -> f64 {
    match kind {
        ReduceKind::Sum | ReduceKind::Mean => 0.0,
        ReduceKind::Max => f64::NEG_INFINITY,
        ReduceKind::Min => f64::INFINITY,
    }
}

/// Fold one element `x` into `acc` under `kind`.
fn reduce_step(kind: ReduceKind, acc: f64, x: f64) -> f64 {
    match kind {
        ReduceKind::Sum | ReduceKind::Mean => acc + x,
        ReduceKind::Max => acc.max(x),
        ReduceKind::Min => acc.min(x),
    }
}

/// Advance `coord` to the next C-order (row-major) coordinate within `dims`,
/// wrapping to all zeros after the last one.
fn advance_c_order(coord: &mut [usize], dims: &[usize]) {
    for d in (0..dims.len()).rev() {
        coord[d] += 1;
        if coord[d] < dims[d] {
            break;
        }
        coord[d] = 0;
    }
}

/// The subset of the SOURCE strides that correspond to the axes that SURVIVE a
/// reduction over `axis` — indexed in output-coordinate order. Lets a reduce map an
/// output coordinate to its base flat offset in the source.
fn strides_including_removed(shape: &[usize], axis: usize) -> Vec<usize> {
    let src = strides(shape);
    src.iter()
        .enumerate()
        .filter(|&(d, _)| d != axis)
        .map(|(_, &s)| s)
        .collect()
}

/// Gather a hyper-rectangle out of a row-major `data` buffer. `out_shape[d] = end-start`
/// per axis, `ranges[d] = (start,end)`, `src_strides` are the source's C-order strides.
fn gather<T: Copy>(
    data: &[T],
    out_shape: &[usize],
    ranges: &[(usize, usize)],
    src_strides: &[usize],
) -> Vec<T> {
    let total: usize = out_shape.iter().product();
    let ndim = out_shape.len();
    let mut out = Vec::with_capacity(total);
    let mut coord = vec![0usize; ndim];
    for _ in 0..total {
        let mut src = 0usize;
        for d in 0..ndim {
            src += (ranges[d].0 + coord[d]) * src_strides[d];
        }
        out.push(data[src]);
        advance_c_order(&mut coord, out_shape);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t_2x3_f32() -> Tensor {
        // [[1,2,3],[4,5,6]]
        Tensor::new(vec![2, 3], Buffer::F32(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0])).unwrap()
    }

    #[test]
    fn new_rejects_len_mismatch() {
        assert!(Tensor::new(vec![2, 3], Buffer::F32(vec![1.0, 2.0])).is_err());
        let t = t_2x3_f32();
        assert_eq!(t.dtype, DType::F32);
        assert_eq!(t.numel(), 6);
        assert_eq!(t.ndim(), 2);
    }

    #[test]
    fn slice_extracts_subrect() {
        let t = t_2x3_f32();
        // columns 1..3 of both rows → [[2,3],[5,6]]
        let s = t.slice(&[(0, 2), (1, 3)]).unwrap();
        assert_eq!(s.shape, vec![2, 2]);
        assert_eq!(s.data, Buffer::F32(vec![2.0, 3.0, 5.0, 6.0]));
        // single row
        let r = t.slice(&[(1, 2), (0, 3)]).unwrap();
        assert_eq!(r.shape, vec![1, 3]);
        assert_eq!(r.data, Buffer::F32(vec![4.0, 5.0, 6.0]));
    }

    #[test]
    fn slice_bounds_checked() {
        let t = t_2x3_f32();
        assert!(t.slice(&[(0, 2)]).is_err()); // wrong arity
        assert!(t.slice(&[(0, 2), (0, 4)]).is_err()); // out of bounds
        assert!(t.slice(&[(2, 1), (0, 3)]).is_err()); // start > end
    }

    #[test]
    fn reduce_over_axis() {
        let t = t_2x3_f32(); // [[1,2,3],[4,5,6]]
                             // sum over axis 0 (rows) → [5,7,9]
        let s0 = t.reduce(0, ReduceKind::Sum).unwrap();
        assert_eq!(s0.shape, vec![3]);
        assert_eq!(s0.data, Buffer::F32(vec![5.0, 7.0, 9.0]));
        // sum over axis 1 (cols) → [6,15]
        let s1 = t.reduce(1, ReduceKind::Sum).unwrap();
        assert_eq!(s1.shape, vec![2]);
        assert_eq!(s1.data, Buffer::F32(vec![6.0, 15.0]));
        // mean over axis 1 → [2,5]
        let m1 = t.reduce(1, ReduceKind::Mean).unwrap();
        assert_eq!(m1.data, Buffer::F32(vec![2.0, 5.0]));
        // max/min over axis 0 → [4,5,6] / [1,2,3]
        assert_eq!(
            t.reduce(0, ReduceKind::Max).unwrap().data,
            Buffer::F32(vec![4.0, 5.0, 6.0])
        );
        assert_eq!(
            t.reduce(0, ReduceKind::Min).unwrap().data,
            Buffer::F32(vec![1.0, 2.0, 3.0])
        );
        assert!(t.reduce(2, ReduceKind::Sum).is_err());
    }

    #[test]
    fn reduce_3d_middle_axis() {
        // shape [2,2,2], values 0..8 row-major
        let t = Tensor::new(vec![2, 2, 2], Buffer::I64((0..8).collect::<Vec<i64>>())).unwrap();
        // sum over axis 1: out shape [2,2].
        // element (i,k) = src(i,0,k)+src(i,1,k)
        // src flat = i*4 + j*2 + k
        // (0,0)=0+2=2 (0,1)=1+3=4 (1,0)=4+6=10 (1,1)=5+7=12
        let s = t.reduce(1, ReduceKind::Sum).unwrap();
        assert_eq!(s.shape, vec![2, 2]);
        assert_eq!(s.data, Buffer::I64(vec![2, 4, 10, 12]));
    }

    #[test]
    fn reduce_3d_outer_axes_and_every_kind() {
        // shape [2,2,2], values 0..8 row-major: src(i,j,k) = 4i + 2j + k.
        let t = Tensor::new(vec![2, 2, 2], Buffer::F64((0..8).map(f64::from).collect())).unwrap();
        // max over axis 2: (i,j) -> 4i + 2j + 1.
        let max2 = t.reduce(2, ReduceKind::Max).unwrap();
        assert_eq!(max2.shape, vec![2, 2]);
        assert_eq!(max2.data, Buffer::F64(vec![1.0, 3.0, 5.0, 7.0]));
        // min over axis 2: (i,j) -> 4i + 2j.
        let min2 = t.reduce(2, ReduceKind::Min).unwrap();
        assert_eq!(min2.data, Buffer::F64(vec![0.0, 2.0, 4.0, 6.0]));
        // mean over axis 0: (j,k) -> 2j + k + 2.
        let mean0 = t.reduce(0, ReduceKind::Mean).unwrap();
        assert_eq!(mean0.shape, vec![2, 2]);
        assert_eq!(mean0.data, Buffer::F64(vec![2.0, 3.0, 4.0, 5.0]));
        // sum over axis 0: (j,k) -> 2(2j + k) + 4.
        let sum0 = t.reduce(0, ReduceKind::Sum).unwrap();
        assert_eq!(sum0.data, Buffer::F64(vec![4.0, 6.0, 8.0, 10.0]));
    }

    #[test]
    fn reduce_over_an_empty_axis_yields_each_kinds_identity() {
        let t = Tensor::new(vec![2, 0], Buffer::F64(Vec::new())).unwrap();
        let reduced = |kind| t.reduce(1, kind).unwrap();
        assert_eq!(reduced(ReduceKind::Sum).data, Buffer::F64(vec![0.0, 0.0]));
        // Mean over zero elements stays at the additive identity (no 0/0).
        assert_eq!(reduced(ReduceKind::Mean).data, Buffer::F64(vec![0.0, 0.0]));
        assert_eq!(
            reduced(ReduceKind::Max).data,
            Buffer::F64(vec![f64::NEG_INFINITY; 2])
        );
        assert_eq!(
            reduced(ReduceKind::Min).data,
            Buffer::F64(vec![f64::INFINITY; 2])
        );
        // A 1-D tensor reduces to a rank-0 scalar.
        let v = Tensor::new(vec![3], Buffer::F64(vec![2.0, -1.0, 5.0])).unwrap();
        let s = v.reduce(0, ReduceKind::Max).unwrap();
        assert!(s.shape.is_empty());
        assert_eq!(s.data, Buffer::F64(vec![5.0]));
    }

    #[test]
    fn elementwise_scalar() {
        let t = t_2x3_f32();
        let d = t.elementwise(ElementwiseOp::Mul, 2.0);
        assert_eq!(d.data, Buffer::F32(vec![2.0, 4.0, 6.0, 8.0, 10.0, 12.0]));
        assert_eq!(d.shape, vec![2, 3]);
        let a = t.elementwise(ElementwiseOp::Add, 10.0);
        assert_eq!(
            a.data,
            Buffer::F32(vec![11.0, 12.0, 13.0, 14.0, 15.0, 16.0])
        );
    }

    #[test]
    fn reshape_preserves_buffer() {
        let t = t_2x3_f32();
        let r = t.reshape(vec![3, 2]).unwrap();
        assert_eq!(r.shape, vec![3, 2]);
        assert_eq!(r.data, t.data);
        let flat = t.reshape(vec![6]).unwrap();
        assert_eq!(flat.shape, vec![6]);
        assert!(t.reshape(vec![4, 2]).is_err());
    }

    #[test]
    fn serde_round_trip() {
        let t = t_2x3_f32();
        let json = serde_json::to_string(&t).unwrap();
        let back: Tensor = serde_json::from_str(&json).unwrap();
        assert_eq!(t, back);
    }
}
