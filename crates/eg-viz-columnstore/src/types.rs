//! The value-carrying ingest surface (D-VZ-1 lane V1) — what V0's abstract
//! `ColumnStoreIngest::ingest_rows` deliberately left out (see that trait's doc:
//! it only ever carries `&[ColumnDescriptor]` + a `row_count`, never values). A
//! real ColumnStore needs a real values-in API; [`ColumnData`]/[`ColumnInput`] are
//! it, and [`crate::store::ColumnStore::ingest_columns`] is the entry point.

use eg_viz_core::ColumnType;

/// One column's actual values, row-count-parallel across every column in one
/// [`ColumnInput`] batch. Mirrors the physical value universe `eg-numeric`
/// (`ArrayD<f64>`/`f32`), `eg-plan::RowSet` (id strings + `f32` scores), and Arrow
/// (`Float64Array`/`Int64Array`/`StringArray`/`BooleanArray`) all materialize to —
/// this crate never depends on those crates themselves, but every bridge in
/// [`crate::store`]/[`crate::arrow_ipc`] converts into exactly one of these
/// variants.
#[derive(Clone, Debug, PartialEq)]
pub enum ColumnData {
    F64(Vec<f64>),
    F32(Vec<f32>),
    I64(Vec<i64>),
    U64(Vec<u64>),
    Utf8(Vec<String>),
    Bool(Vec<bool>),
    /// Raw string values for a dictionary-encoded column — [`crate::store`]
    /// interns each value into the column's shared [`crate::dictionary::Dictionary`]
    /// at ingest time; this variant never stores codes itself.
    Categorical(Vec<String>),
}

impl ColumnData {
    pub fn len(&self) -> usize {
        match self {
            Self::F64(v) => v.len(),
            Self::F32(v) => v.len(),
            Self::I64(v) => v.len(),
            Self::U64(v) => v.len(),
            Self::Utf8(v) => v.len(),
            Self::Bool(v) => v.len(),
            Self::Categorical(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The [`ColumnType`] this variant carries — used to cross-check a caller's
    /// declared `logical_type` against the actual data (see
    /// [`crate::error::ColumnStoreError::TypeMismatch`]).
    pub fn logical_type(&self) -> ColumnType {
        match self {
            Self::F64(_) => ColumnType::F64,
            Self::F32(_) => ColumnType::F32,
            Self::I64(_) => ColumnType::I64,
            Self::U64(_) => ColumnType::U64,
            Self::Utf8(_) => ColumnType::Utf8,
            Self::Bool(_) => ColumnType::Bool,
            Self::Categorical(_) => ColumnType::Categorical,
        }
    }
}

/// One column's full ingest description: name, declared type/nullability, the
/// actual values, and (for a nullable column) a parallel validity mask —
/// `true` = valid, `false` = null. `validity` must be `None` for a non-nullable
/// column and, if `Some`, must be the same length as `data`.
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnInput {
    pub name: String,
    pub logical_type: ColumnType,
    pub nullable: bool,
    pub data: ColumnData,
    pub validity: Option<Vec<bool>>,
}

impl ColumnInput {
    pub fn new(name: impl Into<String>, data: ColumnData) -> Self {
        let logical_type = data.logical_type();
        Self {
            name: name.into(),
            logical_type,
            nullable: false,
            data,
            validity: None,
        }
    }

    pub fn nullable(mut self, validity: Vec<bool>) -> Self {
        self.nullable = true;
        self.validity = Some(validity);
        self
    }
}
