//! Columnar (struct-of-arrays) segment for analytical scans (CONCEPT:EG-KG.temporal.columnar-schema-inference).
//!
//! Where `store.rs` chunks a series row-wise (one `Point` = one row of field
//! values) and `arrow_seg.rs` projects a window INTO Arrow/DataFusion, this module
//! is the NATIVE columnar tier: a batch of rows laid out struct-of-arrays — one
//! contiguous typed `Vec` per column plus a packed validity bitmap — so an
//! analytical scan touches ONE column's bytes without materializing whole rows.
//!
//! It is pure-Rust and `serde`-serializable (no Arrow, no DataFusion, no redb), so
//! it compiles in the lean / Pi build exactly like `point`/`query` and persists via
//! the same `rmp-serde` codec the store uses for chunk blobs.
//!
//! ## The columnar payoff (why struct-of-arrays)
//! * A `SUM(value)` / `AVG(value)` / min-max scan reads only the `value` column's
//!   contiguous `Vec<f64>` — cache-friendly, no per-row branch over unrelated
//!   columns, and it can be handed to a SIMD-friendly fold.
//! * NULLs are a side bitmap (1 bit/row), not a sentinel in the value vector, so a
//!   dense `f64` column stays dense and a `MIN`/`MAX` skips nulls in one pass.
//!
//! ## API
//! * [`ColumnarSegment::from_rows`] — build from a schema + row tuples (the
//!   ingest/materialize path).
//! * [`ColumnarSegment::from_points`] — build straight from stored [`Point`]s
//!   (`ts` + one column per field).
//! * Typed column iterators ([`Column::iter_i64`] / `iter_f64` / `iter_bool` /
//!   `iter_str`), each yielding `Option<T>` so nulls are explicit — the
//!   row-materialization-free analytical scan.

use serde::{Deserialize, Serialize};

use crate::point::Point;

/// The scalar type a columnar [`Column`] holds. Mirrors the field kinds the store's
/// `Point` (f64 fields) and a general relational batch need (CONCEPT:EG-KG.temporal.columnar-schema-inference).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColType {
    /// 64-bit signed integer (e.g. a timestamp column).
    I64,
    /// 64-bit float (the `Point` field type).
    F64,
    /// Boolean.
    Bool,
    /// UTF-8 string.
    Str,
}

/// One scalar cell used when *building* a segment from rows, or reading one back.
/// The columnar store never holds these — it explodes them into per-column vectors —
/// but they are the row-shaped currency the build/materialize API speaks
/// (CONCEPT:EG-KG.temporal.columnar-schema-inference).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CellValue {
    I64(i64),
    F64(f64),
    Bool(bool),
    Str(String),
    /// A missing value — recorded in the column's null bitmap, not the value vec.
    Null,
}

impl CellValue {
    /// The [`ColType`] a non-null cell contributes to schema inference. `Null` has no
    /// type of its own (it takes the column's declared type).
    fn col_type(&self) -> Option<ColType> {
        match self {
            CellValue::I64(_) => Some(ColType::I64),
            CellValue::F64(_) => Some(ColType::F64),
            CellValue::Bool(_) => Some(ColType::Bool),
            CellValue::Str(_) => Some(ColType::Str),
            CellValue::Null => None,
        }
    }
}

/// A packed validity bitmap: 1 bit per row, `true` (bit set) = the row's value is
/// present, `false` = NULL (CONCEPT:EG-KG.temporal.columnar-schema-inference). Kept beside each column's dense value
/// vector so nulls never perturb the contiguous typed data.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NullBitmap {
    /// One bit per row, LSB-first within each `u64` word.
    words: Vec<u64>,
    /// Logical bit length (rows), since the last word is over-allocated.
    len: usize,
}

impl NullBitmap {
    /// A bitmap of `len` rows with EVERY row valid (no nulls) — the common dense case.
    pub fn all_valid(len: usize) -> Self {
        let n_words = len.div_ceil(64);
        let mut words = vec![u64::MAX; n_words];
        // Clear the padding bits beyond `len` in the final word so `count_valid` is exact.
        if !len.is_multiple_of(64) {
            if let Some(last) = words.last_mut() {
                let used = len % 64;
                *last = (1u64 << used) - 1;
            }
        }
        Self { words, len }
    }

    /// An all-NULL bitmap of `len` rows.
    pub fn all_null(len: usize) -> Self {
        Self {
            words: vec![0; len.div_ceil(64)],
            len,
        }
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the bitmap covers zero rows.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Mark row `i` NULL (clear its validity bit).
    fn set_null(&mut self, i: usize) {
        if i < self.len {
            self.words[i / 64] &= !(1u64 << (i % 64));
        }
    }

    /// Whether row `i`'s value is present (non-null). Out-of-range ⇒ `false`.
    pub fn is_valid(&self, i: usize) -> bool {
        i < self.len && (self.words[i / 64] >> (i % 64)) & 1 == 1
    }

    /// Count of present (non-null) rows.
    pub fn count_valid(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }
}

/// A single column's dense typed values, struct-of-arrays style (CONCEPT:EG-KG.temporal.columnar-schema-inference).
/// A NULL row still occupies a slot (a type-default placeholder); the segment's null
/// bitmap is authoritative for presence, so the value vector stays contiguous/dense.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ColumnData {
    I64(Vec<i64>),
    F64(Vec<f64>),
    Bool(Vec<bool>),
    Str(Vec<String>),
}

impl ColumnData {
    /// The declared type of this column.
    pub fn col_type(&self) -> ColType {
        match self {
            ColumnData::I64(_) => ColType::I64,
            ColumnData::F64(_) => ColType::F64,
            ColumnData::Bool(_) => ColType::Bool,
            ColumnData::Str(_) => ColType::Str,
        }
    }

    /// Row count (length of the dense value vector).
    pub fn len(&self) -> usize {
        match self {
            ColumnData::I64(v) => v.len(),
            ColumnData::F64(v) => v.len(),
            ColumnData::Bool(v) => v.len(),
            ColumnData::Str(v) => v.len(),
        }
    }

    /// Whether the value vector holds zero rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// An empty typed vector for `ty`.
    fn empty(ty: ColType) -> Self {
        match ty {
            ColType::I64 => ColumnData::I64(Vec::new()),
            ColType::F64 => ColumnData::F64(Vec::new()),
            ColType::Bool => ColumnData::Bool(Vec::new()),
            ColType::Str => ColumnData::Str(Vec::new()),
        }
    }

    /// Push a cell, recording a type-default placeholder for `Null`. Returns `Err`
    /// when the cell's concrete type disagrees with the column's type.
    fn push(&mut self, cell: &CellValue) -> Result<(), String> {
        match (self, cell) {
            (ColumnData::I64(v), CellValue::I64(x)) => v.push(*x),
            (ColumnData::I64(v), CellValue::Null) => v.push(0),
            (ColumnData::F64(v), CellValue::F64(x)) => v.push(*x),
            (ColumnData::F64(v), CellValue::Null) => v.push(0.0),
            (ColumnData::Bool(v), CellValue::Bool(x)) => v.push(*x),
            (ColumnData::Bool(v), CellValue::Null) => v.push(false),
            (ColumnData::Str(v), CellValue::Str(x)) => v.push(x.clone()),
            (ColumnData::Str(v), CellValue::Null) => v.push(String::new()),
            (this, other) => {
                return Err(format!(
                    "column type {:?} cannot accept cell {other:?}",
                    this.col_type()
                ))
            }
        }
        Ok(())
    }
}

/// One named column: its dense value vector + a validity bitmap (CONCEPT:EG-KG.temporal.columnar-schema-inference).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data: ColumnData,
    pub nulls: NullBitmap,
}

impl Column {
    /// The declared scalar type.
    pub fn col_type(&self) -> ColType {
        self.data.col_type()
    }

    /// Row count.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether the column has zero rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether row `i` is present (non-null).
    pub fn is_valid(&self, i: usize) -> bool {
        self.nulls.is_valid(i)
    }

    /// Typed scan over the `i64` column: `Option<i64>` per row (`None` = NULL). Returns
    /// an empty iterator (via `None` type-mismatch guard) if the column is not `I64`.
    /// The row-materialization-free analytical read path (CONCEPT:EG-KG.temporal.columnar-schema-inference).
    pub fn iter_i64(&self) -> impl Iterator<Item = Option<i64>> + '_ {
        let vals = match &self.data {
            ColumnData::I64(v) => Some(v.as_slice()),
            _ => None,
        };
        (0..self.len()).map(move |i| {
            vals.and_then(|v| {
                if self.nulls.is_valid(i) {
                    Some(v[i])
                } else {
                    None
                }
            })
        })
    }

    /// Typed scan over the `f64` column (CONCEPT:EG-KG.temporal.columnar-schema-inference).
    pub fn iter_f64(&self) -> impl Iterator<Item = Option<f64>> + '_ {
        let vals = match &self.data {
            ColumnData::F64(v) => Some(v.as_slice()),
            _ => None,
        };
        (0..self.len()).map(move |i| {
            vals.and_then(|v| {
                if self.nulls.is_valid(i) {
                    Some(v[i])
                } else {
                    None
                }
            })
        })
    }

    /// Typed scan over the `bool` column (CONCEPT:EG-KG.temporal.columnar-schema-inference).
    pub fn iter_bool(&self) -> impl Iterator<Item = Option<bool>> + '_ {
        let vals = match &self.data {
            ColumnData::Bool(v) => Some(v.as_slice()),
            _ => None,
        };
        (0..self.len()).map(move |i| {
            vals.and_then(|v| {
                if self.nulls.is_valid(i) {
                    Some(v[i])
                } else {
                    None
                }
            })
        })
    }

    /// Typed scan over the `str` column, borrowing each present value (CONCEPT:EG-KG.temporal.columnar-schema-inference).
    pub fn iter_str(&self) -> impl Iterator<Item = Option<&str>> + '_ {
        let vals = match &self.data {
            ColumnData::Str(v) => Some(v.as_slice()),
            _ => None,
        };
        (0..self.len()).map(move |i| {
            vals.and_then(|v| {
                if self.nulls.is_valid(i) {
                    Some(v[i].as_str())
                } else {
                    None
                }
            })
        })
    }

    /// Read one cell back as a [`CellValue`] (`Null` when the row is null). The
    /// row-materializing read — use the typed iterators for analytical scans.
    pub fn value(&self, i: usize) -> CellValue {
        if !self.nulls.is_valid(i) {
            return CellValue::Null;
        }
        match &self.data {
            ColumnData::I64(v) => CellValue::I64(v[i]),
            ColumnData::F64(v) => CellValue::F64(v[i]),
            ColumnData::Bool(v) => CellValue::Bool(v[i]),
            ColumnData::Str(v) => CellValue::Str(v[i].clone()),
        }
    }
}

/// One column of a segment schema: a name + its scalar type (CONCEPT:EG-KG.temporal.columnar-schema-inference).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldDef {
    pub name: String,
    pub ty: ColType,
}

impl FieldDef {
    pub fn new(name: impl Into<String>, ty: ColType) -> Self {
        Self {
            name: name.into(),
            ty,
        }
    }
}

/// A struct-of-arrays batch of rows: one [`Column`] per field + a shared row count
/// (CONCEPT:EG-KG.temporal.columnar-schema-inference). Serde-serializable so it persists via the store's `rmp-serde`
/// chunk codec.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnarSegment {
    pub columns: Vec<Column>,
    pub n_rows: usize,
}

impl ColumnarSegment {
    /// Build a segment from a schema + row tuples (CONCEPT:EG-KG.temporal.columnar-schema-inference). Each row must have
    /// exactly `schema.len()` cells; a cell's concrete type must match its column's
    /// declared type (or be `Null`). NULL cells set the column's validity bit to 0 and
    /// store a type-default placeholder so the value vector stays dense.
    pub fn from_rows(schema: &[FieldDef], rows: &[Vec<CellValue>]) -> Result<Self, String> {
        let n_rows = rows.len();
        // One dense value vec + one bitmap (start all-valid, clear on NULL) per column.
        let mut datas: Vec<ColumnData> = schema.iter().map(|f| ColumnData::empty(f.ty)).collect();
        let mut bitmaps: Vec<NullBitmap> = schema
            .iter()
            .map(|_| NullBitmap::all_valid(n_rows))
            .collect();

        for (r, row) in rows.iter().enumerate() {
            if row.len() != schema.len() {
                return Err(format!(
                    "row {r} has {} cells, schema has {} columns",
                    row.len(),
                    schema.len()
                ));
            }
            for (c, cell) in row.iter().enumerate() {
                datas[c].push(cell)?;
                if matches!(cell, CellValue::Null) {
                    bitmaps[c].set_null(r);
                }
            }
        }

        let columns = schema
            .iter()
            .zip(datas)
            .zip(bitmaps)
            .map(|((f, data), nulls)| Column {
                name: f.name.clone(),
                data,
                nulls,
            })
            .collect();
        Ok(ColumnarSegment { columns, n_rows })
    }

    /// Infer a schema from the FIRST non-null cell of each column of `rows`, then build.
    /// A column that is entirely NULL defaults to [`ColType::F64`] (the series field
    /// type). Convenience over [`from_rows`](Self::from_rows) when the caller has row
    /// data but no explicit schema (CONCEPT:EG-KG.temporal.columnar-schema-inference).
    pub fn from_rows_inferred(names: &[String], rows: &[Vec<CellValue>]) -> Result<Self, String> {
        let mut types = vec![None; names.len()];
        for row in rows {
            if row.len() != names.len() {
                return Err(format!(
                    "row has {} cells, {} column names given",
                    row.len(),
                    names.len()
                ));
            }
            for (c, cell) in row.iter().enumerate() {
                if types[c].is_none() {
                    types[c] = cell.col_type();
                }
            }
        }
        let schema: Vec<FieldDef> = names
            .iter()
            .zip(types)
            .map(|(name, ty)| FieldDef::new(name.clone(), ty.unwrap_or(ColType::F64)))
            .collect();
        Self::from_rows(&schema, rows)
    }

    /// Build a segment straight from stored [`Point`]s (CONCEPT:EG-KG.temporal.columnar-schema-inference): a `ts: I64`
    /// column plus one `F64` column per field (`f0`, `f1`, …). All points must share
    /// the same field count; a point shorter than the max is NULL-padded. The
    /// columnar projection of a series window for analytical scans (SUM/AVG/min-max
    /// over `f0` without touching `ts`).
    pub fn from_points(points: &[Point]) -> Result<Self, String> {
        let n_fields = points.iter().map(|p| p.values.len()).max().unwrap_or(0);
        let mut schema = Vec::with_capacity(n_fields + 1);
        schema.push(FieldDef::new("ts", ColType::I64));
        for f in 0..n_fields {
            schema.push(FieldDef::new(format!("f{f}"), ColType::F64));
        }
        let rows: Vec<Vec<CellValue>> = points
            .iter()
            .map(|p| {
                let mut row = Vec::with_capacity(n_fields + 1);
                row.push(CellValue::I64(p.ts));
                for f in 0..n_fields {
                    row.push(match p.values.get(f) {
                        Some(v) => CellValue::F64(*v),
                        None => CellValue::Null,
                    });
                }
                row
            })
            .collect();
        Self::from_rows(&schema, &rows)
    }

    /// Row count.
    pub fn len(&self) -> usize {
        self.n_rows
    }

    /// Whether the segment has zero rows.
    pub fn is_empty(&self) -> bool {
        self.n_rows == 0
    }

    /// Borrow a column by name — the analytical entry point (a scan projects ONE
    /// column and iterates it, never materializing a row) (CONCEPT:EG-KG.temporal.columnar-schema-inference).
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// The schema (name + type per column).
    pub fn schema(&self) -> Vec<FieldDef> {
        self.columns
            .iter()
            .map(|c| FieldDef::new(c.name.clone(), c.col_type()))
            .collect()
    }

    /// Serialize to the store's `rmp-serde` chunk encoding so a columnar segment
    /// persists beside the row-wise chunks (CONCEPT:EG-KG.temporal.columnar-schema-inference).
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        rmp_serde::to_vec(self).map_err(|e| format!("columnar segment encode: {e}"))
    }

    /// Reconstruct a segment from its persisted `rmp-serde` bytes (CONCEPT:EG-KG.temporal.columnar-schema-inference).
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        rmp_serde::from_slice(bytes).map_err(|e| format!("columnar segment decode: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> Vec<FieldDef> {
        vec![
            FieldDef::new("ts", ColType::I64),
            FieldDef::new("value", ColType::F64),
            FieldDef::new("label", ColType::Str),
            FieldDef::new("ok", ColType::Bool),
        ]
    }

    fn rows() -> Vec<Vec<CellValue>> {
        vec![
            vec![
                CellValue::I64(10),
                CellValue::F64(1.5),
                CellValue::Str("a".into()),
                CellValue::Bool(true),
            ],
            vec![
                CellValue::I64(20),
                CellValue::Null, // a NULL value cell
                CellValue::Str("b".into()),
                CellValue::Bool(false),
            ],
            vec![
                CellValue::I64(30),
                CellValue::F64(3.5),
                CellValue::Null, // a NULL string cell
                CellValue::Bool(true),
            ],
        ]
    }

    /// CONCEPT:EG-KG.temporal.columnar-schema-inference — build a struct-of-arrays segment from rows and read each
    /// typed column back through its zero-row-materialization iterator.
    #[test]
    fn eg_089_columnar_build_from_rows_and_typed_scan() {
        let seg = ColumnarSegment::from_rows(&schema(), &rows()).unwrap();
        assert_eq!(seg.len(), 3);
        assert_eq!(seg.columns.len(), 4);

        let ts: Vec<Option<i64>> = seg.column("ts").unwrap().iter_i64().collect();
        assert_eq!(ts, vec![Some(10), Some(20), Some(30)]);

        let value: Vec<Option<f64>> = seg.column("value").unwrap().iter_f64().collect();
        assert_eq!(value, vec![Some(1.5), None, Some(3.5)]);

        let label: Vec<Option<&str>> = seg.column("label").unwrap().iter_str().collect();
        assert_eq!(label, vec![Some("a"), Some("b"), None]);

        let ok: Vec<Option<bool>> = seg.column("ok").unwrap().iter_bool().collect();
        assert_eq!(ok, vec![Some(true), Some(false), Some(true)]);
    }

    /// CONCEPT:EG-KG.temporal.columnar-schema-inference — the null bitmap is authoritative for presence; a NULL slot
    /// still holds a dense type-default so the value vector never gets a sentinel.
    #[test]
    fn eg_089_columnar_null_bitmap_handling() {
        let seg = ColumnarSegment::from_rows(&schema(), &rows()).unwrap();
        let value = seg.column("value").unwrap();
        assert!(value.is_valid(0));
        assert!(!value.is_valid(1)); // the NULL cell
        assert!(value.is_valid(2));
        assert_eq!(value.nulls.count_valid(), 2);
        assert_eq!(value.value(1), CellValue::Null);

        // A dense f64 fold that skips nulls reads only this column's contiguous vec.
        let sum: f64 = value.iter_f64().flatten().sum();
        assert_eq!(sum, 5.0);
    }

    /// CONCEPT:EG-KG.temporal.columnar-schema-inference — a segment serde round-trips through the `rmp-serde` chunk
    /// codec so it persists beside the store's row-wise chunks.
    #[test]
    fn eg_089_columnar_persist_round_trip() {
        let seg = ColumnarSegment::from_rows(&schema(), &rows()).unwrap();
        let bytes = seg.to_bytes().unwrap();
        let back = ColumnarSegment::from_bytes(&bytes).unwrap();
        assert_eq!(seg, back);
    }

    /// CONCEPT:EG-KG.temporal.columnar-schema-inference — build a columnar segment straight from stored `Point`s: a
    /// `ts` column + one `f*` column per field, NULL-padding a short point.
    #[test]
    fn eg_089_columnar_from_points() {
        let points = vec![
            Point {
                ts: 1,
                values: vec![10.0, 100.0],
            },
            Point {
                ts: 2,
                values: vec![20.0], // short → f1 is NULL-padded
            },
        ];
        let seg = ColumnarSegment::from_points(&points).unwrap();
        assert_eq!(seg.len(), 2);
        let ts: Vec<Option<i64>> = seg.column("ts").unwrap().iter_i64().collect();
        assert_eq!(ts, vec![Some(1), Some(2)]);
        let f1: Vec<Option<f64>> = seg.column("f1").unwrap().iter_f64().collect();
        assert_eq!(f1, vec![Some(100.0), None]);
    }

    /// CONCEPT:EG-KG.temporal.columnar-schema-inference — a type-mismatched cell is rejected, and schema inference picks
    /// the first non-null cell's type per column.
    #[test]
    fn eg_089_columnar_type_mismatch_and_inference() {
        let bad = ColumnarSegment::from_rows(
            &[FieldDef::new("n", ColType::I64)],
            &[vec![CellValue::Str("x".into())]],
        );
        assert!(bad.is_err());

        let names = vec!["a".to_string(), "b".to_string()];
        let seg = ColumnarSegment::from_rows_inferred(
            &names,
            &[
                vec![CellValue::Null, CellValue::Str("s".into())],
                vec![CellValue::I64(7), CellValue::Str("t".into())],
            ],
        )
        .unwrap();
        assert_eq!(seg.column("a").unwrap().col_type(), ColType::I64);
        assert_eq!(seg.column("b").unwrap().col_type(), ColType::Str);
        let a: Vec<Option<i64>> = seg.column("a").unwrap().iter_i64().collect();
        assert_eq!(a, vec![None, Some(7)]);
    }
}
