//! `eg-viz-kernels` — LOD/decimation kernels (D-VZ-1 lane V2).
//!
//! Fills the gap `crates/eg-viz-export/src/reduce.rs` documents explicitly as
//! out of its own scope: "one of the three named Tier-1 algorithms (min-max;
//! M4/LTTB are NOT implemented here)". This crate implements the other two,
//! plus a runtime-detected (never compile-time-assumed) AVX2 fast path:
//!
//! - [`m4_reduce`] — the four-point-per-pixel-column algorithm (first/min/max/
//!   last), the standard for visually-lossless time-series downsampling. Used
//!   as the DEFAULT Decimate-tier kernel for `Line`/`Area` marks (see
//!   `eg-viz-export::render::resolve`) because it reconstructs a shape
//!   indistinguishable from the full line at the target pixel width.
//! - [`lttb_reduce`] — Largest Triangle Three Buckets. Selects real, original
//!   data points (never a synthesized aggregate) — used by the interactive
//!   tile path (V3b) where a client hovering/picking a point must see a real
//!   row's data.
//!
//! Both operate on plain `&[f64]` slices (bridged from a
//! `ColumnStore::materialize_f64` output, or any other caller-owned buffer),
//! so this crate depends on nothing beyond `eg-viz-core`'s pure types — no
//! ColumnStore, no render backend, no server.
//!
//! **Runtime SIMD, never compile-time.** See [`simd`]'s module doc: every
//! accelerated path is reached only after a cached `is_x86_feature_detected!`
//! check, so the SAME binary runs correctly (scalar fallback) on a CPU without
//! AVX2 (e.g. this fleet's interactive dev host, which lacks `x86-64-v3`) as on one with it.

pub mod lttb;
pub mod m4;
pub mod simd;

pub use lttb::lttb_reduce;
pub use m4::m4_reduce;
pub use simd::avx2_available;
