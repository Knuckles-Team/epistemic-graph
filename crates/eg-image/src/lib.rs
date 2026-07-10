//! # eg-image — the image modality (metadata/region-level, CONCEPT:E4 follow-up)
//!
//! A pure-Rust leaf crate (a sibling of `eg-tensor` / `eg-geo`) giving the engine an
//! image data type with **NO heavy image-decode dependency** (no `image`/`png`/
//! `jpeg-decoder` crate) — the Raspberry-Pi contract. It is deliberately
//! **metadata/region-level**, not a pixel codec:
//!
//! * [`ImageData`] — `width`/`height` (read from a REAL PNG `IHDR` chunk or JPEG
//!   `SOFn` marker via [`header::read_png_dimensions`]/[`header::read_jpeg_dimensions`]
//!   — never fabricated), a content-addressed `blob_ref` pointing at the original
//!   bytes in the engine's blob CAS, and an optional [`ImageRegion`] index (named
//!   pixel-space bounding boxes) supplied by an external extractor (object detector,
//!   OCR box finder, human annotation, …). This crate never invents a region.
//! * [`header`] — the dependency-free header parsers, plus a duplicated FNV-1a-128
//!   [`header::content_hash`] (the SAME construction `eg_tensor::store::content_hash`
//!   uses) so `ImageData::from_bytes` can content-address real bytes with no new dep
//!   and no cross-leaf-crate dependency (eg-image and eg-tensor are DAG-parallel).
//!
//! ## What this crate does NOT do
//!
//! Full pixel decode (turning PNG/JPEG bytes into an RGB/RGBA buffer) is explicitly
//! OUT of scope for v1 — see the crate's reserved, dependency-free `codec` feature
//! (currently a no-op) for where a real decoder would be gated in a follow-up, kept
//! OUT of the default build so the Pi contract holds (no heavy image-codec dependency
//! ever lands in a default/pi/node/full build unless a caller explicitly opts in).
//!
//! ## `ModalityContract` (CONCEPT:E4 follow-up)
//!
//! Behind the crate's own opt-in `contract` feature (default OFF, mirroring
//! `eg-tensor`/`eg-geo`): `ImageData::evidence(id)` returns a REAL, located
//! `eg_modality::EvidenceSpan::ImageRegion` from the first stored region (or `None`
//! when the image carries no region index yet — never fabricated). See
//! `src/contract.rs`.

mod header;
mod image;

// ModalityContract retrofit (CONCEPT:E4 follow-up): `impl ModalityContract for
// ImageData`, behind the crate's own opt-in `contract` feature (default OFF).
#[cfg(feature = "contract")]
mod contract;

pub use header::{content_hash, read_jpeg_dimensions, read_png_dimensions};
pub use image::{ImageData, ImageFormat, ImageRegion};
