//! `eg-viz-export` — static chart export (D-VZ-1, lane V3a). "The matplotlib
//! replacement, and what agents and reports need first" per the program
//! charter, prioritised over the interactive/WebGPU client (V3b, a later lane).
//!
//! [`backend::ColumnStoreExportBackend`] implements V0's
//! [`eg_viz_core::StaticExportBackend`] trait over an
//! [`eg_viz_columnstore::ColumnStore`]:
//!
//! 1. [`backend::ColumnStoreExportBackend::resolve`] calls
//!    [`eg_viz_core::select_tier`] over the dataset's REAL row count and builds a
//!    [`plan::RenderPlan`] bounded by whatever tier was selected — [`reduce::direct`]
//!    for `Direct`, [`reduce::decimate_m4`] (Line/Area) or [`reduce::decimate_minmax`]
//!    (Bar) for `Decimate`, [`reduce::density_grid`] for `Density` (`Tiled` is out
//!    of this lane's scope — see [`error::ExportError::TieredNotSupportedByStaticExport`]).
//! 2. [`eg_viz_core::StaticExportBackend::export`] renders the cached plan to
//!    `Png`/`Svg`/`Pdf` bytes via [`raster`]+[`png_encoder`], [`svg_writer`], or
//!    [`pdf_writer`] respectively — three hand-rolled, dependency-free encoders
//!    (see each module's doc for why: this lane adds zero new crates.io
//!    dependencies).
//!
//! ★ **The tier decision is never re-derived.** `resolve` is the only call site
//! for `select_tier` in this crate; every downstream step (the three reduction
//! kernels, all three encoders) consumes the tier/plan it already decided.
//!
//! ★ **`ViewResult.exact` is never set here directly** — every result this
//! crate builds goes through `ViewResult::exact`/`ViewResult::reduced`
//! (`eg-viz-core::result`), which compute the bit from `exact_bit_for` in the
//! one place V0 defined it.

pub mod backend;
pub mod error;
pub mod graph_layout;
pub mod pdf_writer;
pub mod plan;
pub mod png_encoder;
pub mod raster;
pub mod reduce;
pub mod render;
pub mod svg_writer;

pub use backend::ColumnStoreExportBackend;
pub use error::ExportError;
pub use plan::{DrawOp, LinearMap, RenderPlan};
