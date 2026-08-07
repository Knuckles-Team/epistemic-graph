//! Errors for `eg-viz-export` (D-VZ-1 lane V3a).

use std::fmt;

#[derive(Debug)]
pub enum ExportError {
    ColumnStore(eg_viz_columnstore::ColumnStoreError),
    /// `ViewSpec::marks` was empty — nothing to render.
    NoMarks,
    /// The primary mark is missing a required `x`/`y` encoding.
    MissingEncoding(&'static str),
    /// `x` and `y` columns materialized to different row counts.
    ColumnLengthMismatch {
        x: usize,
        y: usize,
    },
    /// `LodTier::Tiled` was selected — out-of-core viewport paging is V3b/V4
    /// scope (a live tile cache over a viewport), not a static single-frame
    /// export. A caller hitting this should narrow the query or raise the
    /// frame budget rather than expect V3a to page tiles.
    TieredNotSupportedByStaticExport,
    /// The mark kind has no static-export geometry implemented in this lane
    /// (`Graph` is V6-only per the capability matrix; `Heatmap`'s direct/decimate
    /// tiers need a z-matrix resolution path this lane does not implement).
    UnsupportedMark(eg_viz_core::MarkKind),
    /// `export()` was called with a `ViewResult` this backend never `resolve()`d
    /// (no cached render plan under that `query_hash`) — `export` renders an
    /// already-resolved result, it never re-resolves one.
    NotResolved,
    Digest(String),
    Result(eg_viz_core::ViewResultError),
}

impl fmt::Display for ExportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ColumnStore(e) => write!(f, "column store error: {e}"),
            Self::NoMarks => write!(f, "ViewSpec has no marks to render"),
            Self::MissingEncoding(channel) => {
                write!(f, "primary mark is missing its `{channel}` encoding")
            }
            Self::ColumnLengthMismatch { x, y } => {
                write!(f, "x column has {x} rows but y column has {y} rows")
            }
            Self::TieredNotSupportedByStaticExport => write!(
                f,
                "select_tier chose LodTier::Tiled; out-of-core viewport paging is V3b/V4 scope, not static export"
            ),
            Self::UnsupportedMark(mark) => {
                write!(f, "{mark:?} has no static-export geometry implemented in lane V3a")
            }
            Self::NotResolved => write!(
                f,
                "export() was called with a ViewResult this backend never resolve()d"
            ),
            Self::Digest(msg) => write!(f, "failed to digest ViewSpec: {msg}"),
            Self::Result(e) => write!(f, "invalid ViewResult: {e}"),
        }
    }
}

impl std::error::Error for ExportError {}

impl From<eg_viz_columnstore::ColumnStoreError> for ExportError {
    fn from(e: eg_viz_columnstore::ColumnStoreError) -> Self {
        Self::ColumnStore(e)
    }
}

impl From<eg_viz_core::ViewResultError> for ExportError {
    fn from(e: eg_viz_core::ViewResultError) -> Self {
        Self::Result(e)
    }
}
