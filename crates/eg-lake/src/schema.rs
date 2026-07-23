//! The lakehouse data model: a typed schema, cell values and a row batch
//! (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
//!
//! This is the neutral, dependency-free currency `eg-lake` speaks. The engine's real
//! columnar data — an `eg-tsdb::ColumnarSegment` (CONCEPT:EG-KG.temporal.columnar-schema-inference) or a user-table row
//! batch out of `eg-query`'s `tables/` store (CONCEPT:EG-KG.query.register-user-tables-alongside) — is converted INTO a
//! [`LakeBatch`] at the (documented) engine-side seam, and everything below (Parquet
//! transcode, Delta/Iceberg logs, the catalog) operates purely on this model. Keeping
//! the model here — rather than depending on `eg-tsdb`/`eg-query` — is what lets
//! `eg-lake` stay a true leaf crate with no workspace edges (the Pi contract).
//!
//! The type set is deliberately the intersection of the engine's `ColType`
//! (EG-089) and the user-table `ColumnType` (EG-018), so a conversion from either is
//! total and each variant maps cleanly to ONE Arrow type, ONE Parquet physical type,
//! ONE Delta/Iceberg logical type — a materialized column is never lossy-dropped.

use serde::{Deserialize, Serialize};

/// The scalar type a lakehouse column holds (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns). The intersection of the
/// engine's columnar `ColType` and the user-table `ColumnType`, each mapping 1:1 to an
/// Arrow / Parquet / Delta / Iceberg type (see the per-format `type_name` helpers).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LakeType {
    /// 64-bit signed integer.
    Long,
    /// 64-bit float.
    Double,
    /// Boolean.
    Bool,
    /// UTF-8 string.
    String,
    /// Microsecond-since-epoch timestamp (stored as i64 micros).
    Timestamp,
}

impl LakeType {
    /// The Delta Lake logical type name for this column (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns). Delta's
    /// schema is a JSON string of these primitive names.
    pub fn delta_type_name(self) -> &'static str {
        match self {
            LakeType::Long => "long",
            LakeType::Double => "double",
            LakeType::Bool => "boolean",
            LakeType::String => "string",
            LakeType::Timestamp => "timestamp",
        }
    }

    /// The Apache Iceberg logical type name for this column (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
    pub fn iceberg_type_name(self) -> &'static str {
        match self {
            LakeType::Long => "long",
            LakeType::Double => "double",
            LakeType::Bool => "boolean",
            LakeType::String => "string",
            // Iceberg spells a micros-since-epoch timestamp `timestamptz` when it
            // carries a zone; we emit UTC micros, so the zoned spelling is correct.
            LakeType::Timestamp => "timestamptz",
        }
    }
}

/// One named, typed column in a [`LakeSchema`] (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LakeField {
    pub name: String,
    pub ty: LakeType,
    /// Whether the column admits nulls. Materialized columns default to nullable
    /// (matches the engine's per-cell null bitmap in EG-089).
    pub nullable: bool,
}

impl LakeField {
    pub fn new(name: impl Into<String>, ty: LakeType) -> Self {
        LakeField {
            name: name.into(),
            ty,
            nullable: true,
        }
    }

    pub fn required(name: impl Into<String>, ty: LakeType) -> Self {
        LakeField {
            name: name.into(),
            ty,
            nullable: false,
        }
    }
}

/// An ordered list of typed columns — the schema of a materialized table
/// (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LakeSchema {
    pub fields: Vec<LakeField>,
}

impl LakeSchema {
    pub fn new(fields: Vec<LakeField>) -> Self {
        LakeSchema { fields }
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Index of a column by name.
    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }
}

/// One scalar cell in a row (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns). `Null` records an absent value (the
/// engine's null-bitmap slot in EG-089); the type is carried by the schema, not the
/// cell.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CellValue {
    Long(i64),
    Double(f64),
    Bool(bool),
    String(String),
    /// Micros since the Unix epoch (UTC).
    Timestamp(i64),
    Null,
}

impl CellValue {
    pub fn is_null(&self) -> bool {
        matches!(self, CellValue::Null)
    }
}

/// A row-major batch of typed rows aligned to a [`LakeSchema`] — the unit the
/// engine hands `eg-lake` to materialize (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns). Row-major is the shape the
/// engine's `ColumnarSegment::from_rows` / a user-table `SELECT *` already speak; the
/// Parquet writer explodes it column-major internally.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LakeBatch {
    pub schema: LakeSchema,
    pub rows: Vec<Vec<CellValue>>,
}

impl LakeBatch {
    /// Build a batch, validating every row's arity against the schema
    /// (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns). Cell/column type mismatches are NOT coerced here — the
    /// Parquet writer treats a wrong-typed cell as null for that column, matching the
    /// engine's schema-on-read tolerance; arity is the one hard invariant.
    pub fn new(schema: LakeSchema, rows: Vec<Vec<CellValue>>) -> Result<Self, String> {
        let width = schema.len();
        for (i, row) in rows.iter().enumerate() {
            if row.len() != width {
                return Err(format!(
                    "row {i} has {} cells, schema has {width} columns",
                    row.len()
                ));
            }
        }
        Ok(LakeBatch { schema, rows })
    }

    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}
