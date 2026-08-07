//! Errors for `eg-viz-columnstore` (D-VZ-1 lane V1).

use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnStoreError {
    /// A dataset ref was resolved (`row_count`/`column`/materialize) but nothing
    /// was ever ingested under that ref.
    UnknownDataset(String),
    /// A column name was resolved against a real dataset but that dataset has no
    /// column with that name.
    UnknownColumn { dataset_ref: String, column: String },
    /// `ingest_columns` was given columns whose `ColumnData` row counts disagree —
    /// every column in one ingest call must describe the SAME rows.
    RowCountMismatch {
        column: String,
        expected: u64,
        actual: u64,
    },
    /// A column's `ColumnData` variant does not match its declared `ColumnType`
    /// (e.g. `ColumnType::F64` paired with `ColumnData::Utf8`).
    TypeMismatch {
        column: String,
        declared: eg_viz_core::ColumnType,
    },
    /// `ingest_columns` was called with zero columns — a dataset must describe at
    /// least one column (mirrors `ViewSpec::validate`'s "at least one mark" rule:
    /// an empty ingest is a caller bug, not a valid empty dataset).
    NoColumns,
    /// A column was declared `nullable: false` but a `validity` mask was supplied
    /// with at least one `false` entry — nulls in a non-nullable column are a
    /// caller contract violation, not silently dropped.
    UnexpectedNull { column: String },
    /// Materializing a column whose stored `logical_type` does not match the
    /// requested materialization type (e.g. `materialize_f64` on a `Utf8` column).
    MaterializeTypeMismatch {
        column: String,
        logical_type: eg_viz_core::ColumnType,
    },
    /// A chunk's `content_id` was not found in the store's content-addressed chunk
    /// table — an internal-consistency violation (every `Chunk` a `Column` holds
    /// must have been inserted into `chunk_bytes` at ingest time).
    MissingChunkBytes(String),
    #[cfg(feature = "arrow-ipc")]
    ArrowIpc(String),
}

impl fmt::Display for ColumnStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownDataset(dataset_ref) => {
                write!(f, "unknown dataset_ref `{dataset_ref}`")
            }
            Self::UnknownColumn { dataset_ref, column } => {
                write!(f, "dataset `{dataset_ref}` has no column `{column}`")
            }
            Self::RowCountMismatch {
                column,
                expected,
                actual,
            } => write!(
                f,
                "column `{column}` has {actual} rows, expected {expected} (every column in one ingest call must describe the same rows)"
            ),
            Self::TypeMismatch { column, declared } => write!(
                f,
                "column `{column}` declared {declared:?} but its ColumnData does not match"
            ),
            Self::NoColumns => write!(f, "ingest_columns requires at least one column"),
            Self::UnexpectedNull { column } => write!(
                f,
                "column `{column}` is declared non-nullable but its validity mask contains a null"
            ),
            Self::MaterializeTypeMismatch { column, logical_type } => write!(
                f,
                "column `{column}` is {logical_type:?}, cannot materialize with this accessor"
            ),
            Self::MissingChunkBytes(content_id) => write!(
                f,
                "internal error: chunk content_id `{content_id}` has no stored bytes"
            ),
            #[cfg(feature = "arrow-ipc")]
            Self::ArrowIpc(msg) => write!(f, "arrow IPC ingest failed: {msg}"),
        }
    }
}

impl std::error::Error for ColumnStoreError {}
