//! `VizOp` — the wire op `Method::Viz` carries (D-VZ-1 lanes V4 "engine
//! integration" / V6 "graph-native marks"), gated `viz`. Mirrors
//! `statechart::StatechartOp`/`jobs::JobOp`'s "one `Method` variant, one internal
//! op enum" shape so the whole native-visualization render surface costs the wire
//! protocol exactly ONE new `Method` variant.
//!
//! Pure serde — no dependency on `eg-viz-core`/`eg-viz-columnstore`/`eg-viz-export`
//! (which sit ABOVE this crate in a separate small DAG, gated by the facade's own
//! `viz`/`viz-columnstore`/`viz-static-export` features — see the root
//! `Cargo.toml`). [`VizRenderRequest::spec_json`] carries a caller-provided
//! `eg_viz_core::ViewSpec` as an opaque JSON value rather than the typed struct
//! itself — the SAME "opaque blob decoded at the verified boundary" pattern
//! `StatechartOp::Define`'s `def_msgpack` uses for `eg_statechart::StatechartDef`
//! — keeping `eg-types` the bottom of the crate DAG.
//!
//! **Scope — full V4.** The handler (`src/server/handlers/viz.rs`, facade
//! feature `viz-static-export`) resolves `VizOp::Render` against a PERSISTENT
//! `eg_viz_columnstore::ColumnStore` (not fresh-per-request), honors a
//! content-addressed render cache, and records durable provenance for every
//! produced render, queryable via [`VizOp::RenderProvenance`]. Still never a
//! view over a live query against a resident `GraphCore` — viz remains
//! explicitly NOT graph-scoped (see that handler's module doc).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The native-visualization render control-plane operation (D-VZ-1, full V4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VizOp {
    /// Resolve a `ViewSpec` against a dataset and render it to static image bytes.
    Render(VizRenderRequest),
    /// Fetch the mark x surface capability matrix
    /// (`eg_viz_core::CapabilityMatrix::default_matrix`) — what a planner/UI reads
    /// to know which (mark, surface) pairs are real today.
    CapabilityMatrix,
    /// Look up a previously-produced render's durable provenance (lane V4) by
    /// the `result_ref` a `VizOp::Render` response returned. Answers `None`
    /// (not an error) for an unknown/never-produced `result_ref`.
    RenderProvenance { result_ref: String },
}

/// One `VizOp::Render` request: an opaque `eg_viz_core::ViewSpec` (as JSON — the
/// handler parses + validates it), which dataset to resolve it against, the
/// target canvas size, the output format, and the frame budget
/// `eg_viz_core::select_tier` resolves against.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VizRenderRequest {
    /// A caller-provided `eg_viz_core::ViewSpec`, as JSON. Opaque here (this
    /// crate has no `eg-viz-core` dependency); the handler
    /// `serde_json::from_value`s and `ViewSpec::validate()`s it at the verified
    /// boundary, exactly as documented as unsupported here for silent misparse.
    pub spec_json: serde_json::Value,
    /// `None` ⇒ resolve against a `dataset_ref` this engine already has
    /// resident from an earlier request (lane V4's persistent ColumnStore —
    /// see `src/server/handlers/viz.rs`'s module doc); an unknown
    /// `dataset_ref` with no data supplied is a clear, typed "unavailable"
    /// error, never a fabricated empty render. `Some` ingests (or
    /// re-ingests — content-addressed, so byte-identical data is a cheap
    /// no-op for the render cache) the given data under `dataset_ref` first.
    #[serde(default)]
    pub dataset: Option<VizDatasetSource>,
    pub width_px: u32,
    pub height_px: u32,
    pub format: VizFormat,
    pub max_primitives: u64,
    pub max_bytes: u64,
    pub dataset_ref: String,
}

/// Static export raster/vector format — mirrors `eg_viz_core::StaticExportFormat`
/// field-for-field, INCLUDING its `rename_all = "snake_case"` wire casing (so a
/// caller sends `"png"`/`"svg"`/`"pdf"`, not `"Png"`/`"Svg"`/`"Pdf"`). This crate
/// stays dependency-free (no `eg-viz-core` dep), so the variant is not reused
/// directly; the handler converts one-to-one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VizFormat {
    Png,
    Svg,
    Pdf,
}

/// Where `VizOp::Render` gets its rows from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VizDatasetSource {
    /// Caller-supplied columns, ingested verbatim into a fresh per-request
    /// `ColumnStore` under `VizRenderRequest::dataset_ref`.
    InlineColumns {
        columns: BTreeMap<String, VizColumnValues>,
    },
    /// A deterministic, seeded, ENGINE-SIDE-generated clustered scatter dataset —
    /// never shipped over the wire — proving high-density rendering without a
    /// huge request payload. Produces `x`/`y` F64 columns with `clusters`
    /// distinct Gaussian-ish blobs.
    SyntheticScatterClusters {
        row_count: u64,
        clusters: u32,
        seed: u64,
    },
    /// A deterministic, seeded, ENGINE-SIDE-generated random graph (node ids
    /// `0..node_count`, `edge_count` edges, no self-loops, dedup not required) —
    /// the `MarkKind::Graph` V6-lite demo path. See `src/server/handlers/viz.rs`'s
    /// module doc for the exact dataset/column shape this ingests as.
    SyntheticGraph {
        node_count: u64,
        edge_count: u64,
        seed: u64,
    },
}

/// One column's values for [`VizDatasetSource::InlineColumns`] — mirrors
/// `eg_viz_columnstore::ColumnData`'s two most common variants. This crate stays
/// dependency-free; the handler converts to the real type at the verified
/// boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VizColumnValues {
    F64(Vec<f64>),
    Utf8(Vec<String>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn viz_op_render_round_trips_through_json() {
        let mut columns = BTreeMap::new();
        columns.insert("x".to_string(), VizColumnValues::F64(vec![1.0, 2.0, 3.0]));
        let op = VizOp::Render(VizRenderRequest {
            spec_json: serde_json::json!({"version": 1, "marks": []}),
            dataset: Some(VizDatasetSource::InlineColumns { columns }),
            width_px: 800,
            height_px: 600,
            format: VizFormat::Png,
            max_primitives: 200_000,
            max_bytes: 50_000_000,
            dataset_ref: "ds:1".to_string(),
        });
        let json = serde_json::to_string(&op).unwrap();
        let restored: VizOp = serde_json::from_str(&json).unwrap();
        match restored {
            VizOp::Render(req) => assert_eq!(req.dataset_ref, "ds:1"),
            other => panic!("expected Render, got {other:?}"),
        }
    }

    #[test]
    fn viz_op_render_with_omitted_dataset_round_trips_through_json() {
        // Lane V4: a caller may omit `dataset` to resolve against an
        // already-ingested `dataset_ref` on the engine's persistent
        // ColumnStore, without resending data over the wire.
        let op = VizOp::Render(VizRenderRequest {
            spec_json: serde_json::json!({"version": 1, "marks": []}),
            dataset: None,
            width_px: 800,
            height_px: 600,
            format: VizFormat::Png,
            max_primitives: 200_000,
            max_bytes: 50_000_000,
            dataset_ref: "ds:1".to_string(),
        });
        let json = serde_json::to_string(&op).unwrap();
        let restored: VizOp = serde_json::from_str(&json).unwrap();
        match restored {
            VizOp::Render(req) => assert!(req.dataset.is_none()),
            other => panic!("expected Render, got {other:?}"),
        }
    }

    /// A request missing the `dataset` field entirely (an older client, or a
    /// hand-built request) must still deserialize -- `#[serde(default)]`
    /// makes an absent field the SAME as an explicit `null`, not a decode
    /// error.
    #[test]
    fn viz_render_request_without_a_dataset_field_at_all_still_deserializes() {
        let json = serde_json::json!({
            "spec_json": {"version": 1, "marks": []},
            "width_px": 800,
            "height_px": 600,
            "format": "png",
            "max_primitives": 200_000,
            "max_bytes": 50_000_000,
            "dataset_ref": "ds:1"
        })
        .to_string();
        let req: VizRenderRequest = serde_json::from_str(&json).unwrap();
        assert!(req.dataset.is_none());
    }

    #[test]
    fn viz_op_capability_matrix_round_trips_through_json() {
        let op = VizOp::CapabilityMatrix;
        let json = serde_json::to_string(&op).unwrap();
        let restored: VizOp = serde_json::from_str(&json).unwrap();
        assert!(matches!(restored, VizOp::CapabilityMatrix));
    }

    #[test]
    fn viz_op_render_provenance_round_trips_through_json() {
        let op = VizOp::RenderProvenance {
            result_ref: "eg:viz_result:abc123".to_string(),
        };
        let json = serde_json::to_string(&op).unwrap();
        let restored: VizOp = serde_json::from_str(&json).unwrap();
        match restored {
            VizOp::RenderProvenance { result_ref } => {
                assert_eq!(result_ref, "eg:viz_result:abc123")
            }
            other => panic!("expected RenderProvenance, got {other:?}"),
        }
    }

    #[test]
    fn synthetic_dataset_sources_round_trip_through_json() {
        let scatter = VizDatasetSource::SyntheticScatterClusters {
            row_count: 5_000_000,
            clusters: 5,
            seed: 42,
        };
        let json = serde_json::to_string(&scatter).unwrap();
        let restored: VizDatasetSource = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            restored,
            VizDatasetSource::SyntheticScatterClusters {
                row_count: 5_000_000,
                clusters: 5,
                seed: 42
            }
        ));

        let graph = VizDatasetSource::SyntheticGraph {
            node_count: 100_000,
            edge_count: 300_000,
            seed: 7,
        };
        let json = serde_json::to_string(&graph).unwrap();
        let restored: VizDatasetSource = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            restored,
            VizDatasetSource::SyntheticGraph {
                node_count: 100_000,
                edge_count: 300_000,
                seed: 7
            }
        ));
    }
}
