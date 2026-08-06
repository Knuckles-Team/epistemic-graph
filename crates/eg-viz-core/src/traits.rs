//! Trait boundaries for the pieces later lanes fill in (D-VZ-1 lane V0).
//!
//! **Contracts only — no implementation lives here.** Each trait names exactly one
//! later lane's responsibility per the program charter's lane table
//! (`plans/au-eg-program/PROGRAM.md`):
//!
//! - [`ColumnStoreIngest`] — V1: ingest from `RowSet` (`eg-plan`) / `eg-numeric` /
//!   Arrow into the columnar store the LOD kernels and renderers read.
//! - [`LodKernel`] — V2: the decimation/density/tiling kernels that turn a
//!   [`crate::spec::ViewSpec`] + resolved data into a
//!   [`crate::result::ViewResult`] at the tier [`crate::tier::select_tier`] chose.
//! - [`StaticExportBackend`] — V3a: PNG/SVG/PDF export.
//! - [`InteractiveRenderBackend`] — V3b: the binary tile protocol + WebGPU/WebGL2
//!   client surface.
//!
//! None of these traits pull a dependency: every associated `Error` type is
//! caller-defined, and every method signature uses only this crate's own IR types
//! plus primitives. The `#[cfg(test)]` fakes at the bottom of this file exist
//! solely to prove the trait shapes are implementable and object-usable — they are
//! test-only scaffolding, not a reference implementation, and ship no real
//! ingest/LOD/render behavior (V0's "no ColumnStore implementation, no LOD
//! kernels, no renderer" boundary).

use crate::result::ViewResult;
use crate::spec::MarkKind;
use crate::tier::FrameBudget;

/// A column's logical type, independent of physical encoding — matches the
/// `RowSet`/Arrow/`eg-numeric` value universe this crate's traits bridge without
/// depending on any of those crates directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    F64,
    F32,
    I64,
    U64,
    Utf8,
    Bool,
    /// A finite-cardinality string column, dictionary-encodable (V1's "dictionary
    /// categoricals" per the charter).
    Categorical,
}

/// One column's shape, as V1's ingest boundary needs to describe it.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ColumnDescriptor {
    pub name: String,
    pub logical_type: ColumnType,
    pub nullable: bool,
}

impl ColumnDescriptor {
    pub fn new(name: impl Into<String>, logical_type: ColumnType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            logical_type,
            nullable,
        }
    }
}

/// **V1 boundary.** Ingest tabular data — from a query's `RowSet` result, an
/// `eg-numeric` array, or an Arrow batch — into a durable/content-addressed
/// columnar store keyed by `dataset_ref`, the SAME opaque reference
/// [`crate::spec::MarkSpec::data_ref`] carries. `eg-viz-core` never depends on
/// `eg-plan`/`eg-numeric`/`arrow` itself (V0 is a leaf); V1 implements this trait
/// in a crate that does.
pub trait ColumnStoreIngest {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Ingest `row_count` rows shaped by `columns` under `dataset_ref`, returning
    /// the (possibly rewritten/canonicalized) dataset ref a later
    /// [`LodKernel`]/render call resolves.
    fn ingest_rows(
        &mut self,
        dataset_ref: &str,
        columns: &[ColumnDescriptor],
        row_count: u64,
    ) -> Result<String, Self::Error>;

    /// The row count of an already-ingested dataset — what
    /// [`crate::tier::TierInput::row_count`] is populated from.
    fn row_count(&self, dataset_ref: &str) -> Result<u64, Self::Error>;
}

/// **V2 boundary.** Given the tier [`crate::tier::select_tier`] already chose,
/// produce the reduced/exact [`ViewResult`] for one mark over one dataset. A
/// kernel implementation owns M4/LTTB/min-max decimation, density-grid binning,
/// and tile composition; this trait only names the entry point.
pub trait LodKernel {
    type Error: std::error::Error + Send + Sync + 'static;

    fn reduce(
        &self,
        dataset_ref: &str,
        mark: MarkKind,
        tier: crate::tier::LodTier,
        budget: &FrameBudget,
    ) -> Result<ViewResult, Self::Error>;
}

/// Static export raster/vector formats (V3a — "the matplotlib replacement").
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StaticExportFormat {
    Png,
    Svg,
    Pdf,
}

/// **V3a boundary.** Render an already-reduced [`ViewResult`] (plus the
/// [`crate::spec::ViewSpec`] it answers) to static image bytes. Exactly what
/// agents and reports need first per the charter.
pub trait StaticExportBackend {
    type Error: std::error::Error + Send + Sync + 'static;

    fn export(
        &self,
        spec: &crate::spec::ViewSpec,
        result: &ViewResult,
        format: StaticExportFormat,
    ) -> Result<Vec<u8>, Self::Error>;
}

/// A data-space viewport request, screen-dimensioned — the input an interactive
/// backend resolves a tile/reply against (`xy`'s `density_view`/`view` request
/// shape, `spec/design/wire-protocol.md` §2, adapted).
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Viewport {
    pub x0: f64,
    pub x1: f64,
    pub y0: f64,
    pub y1: f64,
    pub width_px: u32,
    pub height_px: u32,
}

/// **V3b boundary.** Serve one viewport's worth of tile/geometry for the
/// interactive client (binary tile protocol + WebGPU/WebGL2 renderer, pan/zoom/
/// picking — the charter's V3b description).
pub trait InteractiveRenderBackend {
    type Error: std::error::Error + Send + Sync + 'static;

    fn serve_viewport(
        &self,
        dataset_ref: &str,
        viewport: &Viewport,
        budget: &FrameBudget,
    ) -> Result<ViewResult, Self::Error>;
}

#[cfg(test)]
mod tests {
    //! Test-only fakes proving the trait shapes are implementable and usable
    //! through `&dyn Trait` (object safety matters — a V4 planner will hold a
    //! backend behind a trait object to pick among several implementations at
    //! runtime). These are NOT reference implementations: they do no real
    //! ingest/LOD/render work.

    use super::*;
    use crate::result::{PayloadKind, PayloadRef};
    use crate::tier::LodTier;
    use std::collections::HashMap;
    use std::fmt;

    #[derive(Debug)]
    struct FakeError(&'static str);
    impl fmt::Display for FakeError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }
    impl std::error::Error for FakeError {}

    #[derive(Default)]
    struct FakeColumnStore {
        row_counts: HashMap<String, u64>,
    }

    impl ColumnStoreIngest for FakeColumnStore {
        type Error = FakeError;

        fn ingest_rows(
            &mut self,
            dataset_ref: &str,
            _columns: &[ColumnDescriptor],
            row_count: u64,
        ) -> Result<String, Self::Error> {
            self.row_counts.insert(dataset_ref.to_string(), row_count);
            Ok(dataset_ref.to_string())
        }

        fn row_count(&self, dataset_ref: &str) -> Result<u64, Self::Error> {
            self.row_counts
                .get(dataset_ref)
                .copied()
                .ok_or(FakeError("unknown dataset"))
        }
    }

    struct FakeLodKernel;
    impl LodKernel for FakeLodKernel {
        type Error = FakeError;

        fn reduce(
            &self,
            _dataset_ref: &str,
            _mark: MarkKind,
            tier: LodTier,
            _budget: &FrameBudget,
        ) -> Result<ViewResult, Self::Error> {
            if tier == LodTier::Direct {
                ViewResult::exact("qh", 1, Vec::new(), 0, 0).map_err(|_| FakeError("invalid"))
            } else {
                ViewResult::reduced(
                    "qh",
                    0,
                    1,
                    tier,
                    crate::result::ReductionKind::Density,
                    vec![PayloadRef::new("p", PayloadKind::DensityGrid, 0)],
                    0,
                    0,
                )
                .map_err(|_| FakeError("invalid"))
            }
        }
    }

    struct FakeStaticExportBackend;
    impl StaticExportBackend for FakeStaticExportBackend {
        type Error = FakeError;

        fn export(
            &self,
            _spec: &crate::spec::ViewSpec,
            _result: &ViewResult,
            _format: StaticExportFormat,
        ) -> Result<Vec<u8>, Self::Error> {
            Ok(Vec::new())
        }
    }

    struct FakeInteractiveBackend;
    impl InteractiveRenderBackend for FakeInteractiveBackend {
        type Error = FakeError;

        fn serve_viewport(
            &self,
            _dataset_ref: &str,
            _viewport: &Viewport,
            _budget: &FrameBudget,
        ) -> Result<ViewResult, Self::Error> {
            ViewResult::exact("qh", 0, Vec::new(), 0, 0).map_err(|_| FakeError("invalid"))
        }
    }

    #[test]
    fn column_store_ingest_trait_is_usable() {
        let mut store = FakeColumnStore::default();
        let descriptor = ColumnDescriptor::new("x", ColumnType::F64, false);
        let dataset_ref = store.ingest_rows("ds:1", &[descriptor], 42).unwrap();
        assert_eq!(store.row_count(&dataset_ref).unwrap(), 42);
    }

    #[test]
    fn lod_kernel_trait_is_object_safe() {
        let kernel: Box<dyn LodKernel<Error = FakeError>> = Box::new(FakeLodKernel);
        let budget = FrameBudget::new(1, 1);
        let direct = kernel
            .reduce("ds:1", MarkKind::Scatter, LodTier::Direct, &budget)
            .unwrap();
        assert!(direct.exact);
        let density = kernel
            .reduce("ds:1", MarkKind::Scatter, LodTier::Density, &budget)
            .unwrap();
        assert!(!density.exact);
    }

    #[test]
    fn static_export_backend_trait_is_object_safe() {
        let backend: Box<dyn StaticExportBackend<Error = FakeError>> =
            Box::new(FakeStaticExportBackend);
        let spec =
            crate::spec::ViewSpec::new(vec![crate::spec::MarkSpec::new(MarkKind::Line, "ds:1")]);
        let result = ViewResult::exact("qh", 0, Vec::new(), 0, 0).unwrap();
        let bytes = backend
            .export(&spec, &result, StaticExportFormat::Svg)
            .unwrap();
        assert!(bytes.is_empty());
    }

    #[test]
    fn interactive_render_backend_trait_is_object_safe() {
        let backend: Box<dyn InteractiveRenderBackend<Error = FakeError>> =
            Box::new(FakeInteractiveBackend);
        let viewport = Viewport {
            x0: 0.0,
            x1: 1.0,
            y0: 0.0,
            y1: 1.0,
            width_px: 800,
            height_px: 600,
        };
        let budget = FrameBudget::new(1_000, 1_000);
        let result = backend.serve_viewport("ds:1", &viewport, &budget).unwrap();
        assert!(result.exact);
    }
}
