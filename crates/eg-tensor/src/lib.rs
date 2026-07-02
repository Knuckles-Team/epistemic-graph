//! # eg-tensor — the array / tensor modality (CONCEPT:EG-085)
//!
//! A pure-Rust leaf crate (a sibling of `eg-ann` / `eg-geo`) giving the engine a dense
//! N-D array data type + a handful of array ops with **NO BLAS/LAPACK/C dependency** —
//! the Raspberry-Pi contract. It provides:
//!
//! * [`Tensor`] — a `dtype` ([`DType`]: `F32`/`F64`/`I32`/`I64`/`U8`) + a `shape`
//!   (`Vec<usize>`) + a row-major typed data [`Buffer`]. serde-serializable so a
//!   tensor persists as a typed value in the engine's redb per-graph store, and with a
//!   compact byte-blob codec ([`Tensor::to_blob`] / [`Tensor::from_blob`]) so it can be
//!   content-addressed in the existing blob CAS (`ChunkStore` + EG-071 CDC).
//! * Ops: [`Tensor::slice`] (per-axis start..end gather), [`Tensor::reduce`]
//!   ([`ReduceKind`] sum/mean/max/min over one axis), [`Tensor::elementwise`]
//!   ([`ElementwiseOp`] add/sub/mul/div with a scalar), and [`Tensor::reshape`].
//! * [`TensorStore`] — content-addressed, restart-durable persistence (CONCEPT:EG-085):
//!   tensors are kept as content-hashed byte blobs and `persist`/`load` round-trip them
//!   to a directory so a store survives a restart (std-only, no new dep).
//!
//! The wire algebra (`Op::TensorScan`, `Op::TensorOp { kind }`) lives in
//! `eg-types::wire` (pure-serde, Pi-safe); the executor that drives THIS crate lives in
//! `eg-plan::exec` behind eg-plan's `tensor` feature. This crate itself is
//! dependency-light (only serde) and is folded into the `node`/`full` serving tiers,
//! kept OUT of `pi`. Distinct from eg-ann (fixed-D ANN vectors): eg-tensor is dense
//! N-D arrays (images / sensor frames / genomics / ML features).

mod blob;
mod store;
mod tensor;

// CONCEPT:EG-098 — multimodal sensor fusion (robotics/IoT): stack a time-aligned
// multi-rate frame (aligned by `eg_tsdb::fusion`) into a `[timesteps × channels]`
// `Tensor` frame + validity mask, and a windowed variant emitting one fused tensor per
// EG-067 tumbling window. The tensor-building half lives here; the time-alignment half
// lives in eg-tsdb (kept pure-Rust / Pi-lean). Folded into node/full with the tensor
// tier, kept OUT of `pi`.
pub mod fusion;

pub use fusion::{fuse_aligned, fuse_on_grid, windowed_fusion, FusedFrame, WindowFrame};
pub use store::{content_hash, TensorStore};
pub use tensor::{Buffer, DType, ElementwiseOp, ReduceKind, Tensor};
