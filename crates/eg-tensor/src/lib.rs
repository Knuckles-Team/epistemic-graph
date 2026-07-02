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
//!
//! The wire algebra (`Op::TensorScan`, `Op::TensorOp { kind }`) lives in
//! `eg-types::wire` (pure-serde, Pi-safe); the executor that drives THIS crate lives in
//! `eg-plan::exec` behind eg-plan's `tensor` feature. This crate itself is
//! dependency-light (only serde) and is folded into the `node`/`full` serving tiers,
//! kept OUT of `pi`. Distinct from eg-ann (fixed-D ANN vectors): eg-tensor is dense
//! N-D arrays (images / sensor frames / genomics / ML features).

mod blob;
mod tensor;

pub use tensor::{Buffer, DType, ElementwiseOp, ReduceKind, Tensor};
