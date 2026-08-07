//! `ColumnStoreExportBackend` — the concrete `eg-viz-core::StaticExportBackend`
//! implementation (D-VZ-1 lane V3a).
//!
//! V0's trait signature (`export(&self, spec, result, format) -> Vec<u8>`)
//! carries no dataset/canvas-size/budget parameters — those are exactly what a
//! real backend needs beyond the abstract boundary, so this type takes them at
//! construction (`store`, `width_px`, `height_px`) and via the crate-added
//! [`ColumnStoreExportBackend::resolve`] step (`dataset_ref`, `budget`,
//! `snapshot_version`). The split mirrors the architecture diagram
//! (`ColumnStore + LOD engine -> static PNG/SVG/PDF`): [`resolve`] is the
//! "ColumnStore + LOD engine" step — it is the ONLY place [`eg_viz_core::select_tier`]
//! is called and the only place a [`crate::plan::RenderPlan`] is built — and the
//! trait's own `export` is a pure function of an ALREADY-resolved
//! [`eg_viz_core::ViewResult`] plus a format, cached by `query_hash` so it never
//! re-derives a tier or re-scans the store.
//!
//! [`ColumnStoreExportBackend::resolve`]: ColumnStoreExportBackend::resolve

use std::cell::RefCell;
use std::collections::HashMap;

use eg_viz_columnstore::ColumnStore;
use eg_viz_core::{FrameBudget, StaticExportBackend, StaticExportFormat, ViewResult, ViewSpec};

use crate::error::ExportError;
use crate::plan::RenderPlan;
use crate::{pdf_writer, png_encoder, raster, render, svg_writer};

pub struct ColumnStoreExportBackend<'a> {
    store: &'a ColumnStore,
    width_px: u32,
    height_px: u32,
    cache: RefCell<HashMap<String, RenderPlan>>,
}

impl<'a> ColumnStoreExportBackend<'a> {
    pub fn new(store: &'a ColumnStore, width_px: u32, height_px: u32) -> Self {
        Self {
            store,
            width_px,
            height_px,
            cache: RefCell::new(HashMap::new()),
        }
    }

    /// Resolve `spec`'s primary mark against `dataset_ref`: calls
    /// [`eg_viz_core::select_tier`] over the dataset's real row count, builds the
    /// tier-bounded [`RenderPlan`], and caches it under the resulting
    /// [`ViewResult::query_hash`] for a later [`StaticExportBackend::export`] call
    /// to render (never re-resolved).
    pub fn resolve(
        &self,
        spec: &ViewSpec,
        dataset_ref: &str,
        budget: FrameBudget,
        snapshot_version: u64,
    ) -> Result<ViewResult, ExportError> {
        let (result, plan) = render::resolve(
            self.store,
            spec,
            dataset_ref,
            self.width_px,
            self.height_px,
            budget,
            snapshot_version,
        )?;
        self.cache
            .borrow_mut()
            .insert(result.query_hash.clone(), plan);
        Ok(result)
    }

    /// The op count of the plan cached under `query_hash` — how the large-N
    /// tier-honoring proof asserts the render stayed bounded (never
    /// `row_count` draw ops) without needing to decode an exported image back.
    pub fn cached_op_count(&self, query_hash: &str) -> Option<usize> {
        self.cache
            .borrow()
            .get(query_hash)
            .map(|plan| plan.ops.len())
    }
}

impl<'a> StaticExportBackend for ColumnStoreExportBackend<'a> {
    type Error = ExportError;

    fn export(
        &self,
        _spec: &ViewSpec,
        result: &ViewResult,
        format: StaticExportFormat,
    ) -> Result<Vec<u8>, Self::Error> {
        let cache = self.cache.borrow();
        let plan = cache
            .get(&result.query_hash)
            .ok_or(ExportError::NotResolved)?;
        Ok(match format {
            StaticExportFormat::Png => png_encoder::encode(&raster::rasterize(plan)),
            StaticExportFormat::Svg => svg_writer::encode(plan),
            StaticExportFormat::Pdf => pdf_writer::encode(plan),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_viz_columnstore::{ColumnData, ColumnInput};
    use eg_viz_core::{
        EncodingSpec, Encodings, FrameBudget, MarkKind, MarkSpec, ScaleKind, ScaleSpec, ViewSpec,
    };

    fn small_spec() -> ViewSpec {
        let mut spec = ViewSpec::new(vec![MarkSpec::new(MarkKind::Scatter, "ds:1")
            .with_encodings(Encodings {
                x: Some(EncodingSpec::field("x").with_scale("x")),
                y: Some(EncodingSpec::field("y").with_scale("y")),
                ..Default::default()
            })]);
        spec.scales = vec![
            ScaleSpec::new("x", ScaleKind::Linear),
            ScaleSpec::new("y", ScaleKind::Linear),
        ];
        spec
    }

    #[test]
    fn resolve_then_export_produces_all_three_formats() {
        let mut store = ColumnStore::new();
        store
            .ingest_columns(
                "ds:1",
                vec![
                    ColumnInput::new("x", ColumnData::F64(vec![1.0, 2.0, 3.0, 4.0, 5.0])),
                    ColumnInput::new("y", ColumnData::F64(vec![5.0, 3.0, 4.0, 1.0, 2.0])),
                ],
            )
            .unwrap();

        let backend = ColumnStoreExportBackend::new(&store, 80, 60);
        let spec = small_spec();
        let result = backend
            .resolve(
                &spec,
                "ds:1",
                FrameBudget::new(2_000_000, 16 * 1024 * 1024),
                1,
            )
            .unwrap();
        assert!(
            result.exact,
            "5 rows must select LodTier::Direct and be exact"
        );

        let png = backend
            .export(&spec, &result, StaticExportFormat::Png)
            .unwrap();
        assert_eq!(&png[0..4], &[137, 80, 78, 71]);
        let svg = backend
            .export(&spec, &result, StaticExportFormat::Svg)
            .unwrap();
        assert!(String::from_utf8(svg).unwrap().starts_with("<svg"));
        let pdf = backend
            .export(&spec, &result, StaticExportFormat::Pdf)
            .unwrap();
        assert!(pdf.starts_with(b"%PDF-1.4"));
    }

    #[test]
    fn export_before_resolve_is_a_clear_error_not_a_silent_re_resolve() {
        let store = ColumnStore::new();
        let backend = ColumnStoreExportBackend::new(&store, 80, 60);
        let spec = small_spec();
        let fake_result =
            eg_viz_core::ViewResult::exact("eg:viz_query:nonexistent", 0, Vec::new(), 0, 0)
                .unwrap();
        let err = backend
            .export(&spec, &fake_result, StaticExportFormat::Png)
            .unwrap_err();
        assert!(matches!(err, ExportError::NotResolved));
    }
}
