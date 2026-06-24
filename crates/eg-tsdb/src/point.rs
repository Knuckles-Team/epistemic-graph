//! The point/timestamp data model + store error — always compiled (no redb dep) so
//! the native primitives (`query`) and the Time-ops sketch (`time_op`) work even in
//! a build without the `redb-store` feature.

/// Nanoseconds-since-epoch timestamp. `i64` for arithmetic; bucket keys in the store
/// are `u64` to match the engine's ledger `(graph, u64)` key convention.
pub type Ts = i64;

/// One point: a timestamp and one-or-more f64 field values.
#[derive(Clone, Debug, PartialEq)]
pub struct Point {
    pub ts: Ts,
    pub values: Vec<f64>,
}

impl Point {
    pub fn single(ts: Ts, value: f64) -> Self {
        Self {
            ts,
            values: vec![value],
        }
    }
}

/// A typed store/query error.
#[derive(Debug)]
pub enum TsError {
    /// redb table/transaction/storage error.
    Redb(String),
    /// (de)serialization of `SeriesMeta` or a chunk blob failed.
    Codec(String),
    /// A batch mixed `n_fields` widths, or disagreed with the series' stored schema.
    FieldMismatch { expected: usize, got: usize },
}

impl std::fmt::Display for TsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TsError::Redb(m) => write!(f, "tsdb redb error: {m}"),
            TsError::Codec(m) => write!(f, "tsdb codec error: {m}"),
            TsError::FieldMismatch { expected, got } => {
                write!(
                    f,
                    "tsdb field-count mismatch: expected {expected}, got {got}"
                )
            }
        }
    }
}

impl std::error::Error for TsError {}
