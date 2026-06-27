//! Column types, table schemas, and the typed row-cell value (CONCEPT:EG-018).
//!
//! These are the catalog's data model for an ARBITRARY user-defined relational
//! table — the relational sibling of the graph's schema-on-read `nodes` projection.
//! A user `CREATE TABLE` records a [`TableSchema`] (a name + an ordered list of typed
//! [`Column`]s) into the durable catalog; every stored row is a `Vec<Cell>` aligned
//! to that schema's column order. All three types are `serde`-serializable so the
//! redb table store persists them verbatim (MessagePack) and they round-trip across a
//! restart unchanged.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The set of column types a user table column may declare (CONCEPT:EG-018). Chosen
/// to cover the connector / ETL / time-series workloads the engine ingests —
/// Prometheus samples (`timestamp`, `double`), Langfuse spans (`text`, `json`,
/// `timestamp`), stock bars (`bigint`, `double`), and raw connector mirrors
/// (`bytes`, `json`). Kept deliberately small and coarse: every variant maps cleanly
/// to ONE Arrow type and ONE Postgres wire OID, so a SELECT result is never
/// lossy-dropped and a `psql`/ORM client always resolves a sane column type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    /// 32-bit-range integer — stored as i64 (Arrow `Int64`).
    Int,
    /// 64-bit integer (Arrow `Int64`).
    BigInt,
    /// 32-bit float — stored widened to f64 (Arrow `Float64`).
    Float,
    /// 64-bit float (Arrow `Float64`).
    Double,
    /// UTF-8 text (Arrow `Utf8`).
    Text,
    /// Boolean (Arrow `Boolean`).
    Bool,
    /// Microsecond-since-epoch timestamp, stored as i64 (Arrow `Int64`). A native
    /// Arrow temporal type is a follow-up; i64 micros round-trips losslessly today.
    Timestamp,
    /// Opaque byte string (Arrow `Binary`).
    Bytes,
    /// Arbitrary JSON, stored as its canonical text (Arrow `Utf8`).
    Json,
}

impl ColumnType {
    /// Parse a SQL type name (case-insensitive, e.g. from `CREATE TABLE`) into a
    /// [`ColumnType`]. Accepts the common Postgres/standard spellings plus the
    /// engine's canonical names so `int4`/`integer`/`int` all land on `Int`, etc.
    /// Length/precision suffixes (`varchar(255)`, `numeric(10,2)`) are ignored — the
    /// base type is what the store needs. Returns `Err` for an unknown type.
    pub fn parse(name: &str) -> Result<ColumnType, String> {
        // Strip any `(...)` precision/length suffix and whitespace, lowercase.
        let base = name.split('(').next().unwrap_or(name).trim().to_ascii_lowercase();
        let ty = match base.as_str() {
            "int" | "int4" | "integer" | "serial" | "smallint" | "int2" => ColumnType::Int,
            "bigint" | "int8" | "bigserial" | "long" => ColumnType::BigInt,
            "float" | "float4" | "real" => ColumnType::Float,
            "double" | "float8" | "double precision" => ColumnType::Double,
            "text" | "varchar" | "char" | "character" | "character varying" | "string" | "uuid" => {
                ColumnType::Text
            }
            "bool" | "boolean" => ColumnType::Bool,
            "timestamp" | "timestamptz" | "datetime" | "timestamp with time zone"
            | "timestamp without time zone" => ColumnType::Timestamp,
            "bytes" | "bytea" | "blob" | "binary" => ColumnType::Bytes,
            "json" | "jsonb" => ColumnType::Json,
            other => return Err(format!("unsupported column type `{other}`")),
        };
        Ok(ty)
    }
}

/// One column of a user table: its name, declared type, NULL-ability, and whether it
/// participates in the (single, first-pass) primary key. The PK flag is recorded for
/// catalog fidelity and future uniqueness enforcement; the first increment does not
/// yet enforce uniqueness (see the follow-up list).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub ty: ColumnType,
    pub nullable: bool,
    pub primary_key: bool,
}

/// A user table's full schema: its name and ordered columns (CONCEPT:EG-018). The
/// column ORDER is canonical — every stored row's `Vec<Cell>` is aligned to it, and
/// a SELECT projects columns in this order unless the query names them explicitly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
}

impl TableSchema {
    /// The position of `column` in the schema's column order, if present.
    pub fn column_index(&self, column: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == column)
    }

    /// The column with the given name, if present.
    pub fn column(&self, column: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == column)
    }
}

/// One typed cell value of a stored row (CONCEPT:EG-018). A row is a `Vec<Cell>`
/// aligned to the table's column order. `serde`-serializable so the redb store
/// persists rows verbatim and they round-trip exactly across a restart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Cell {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    /// Microseconds since the Unix epoch.
    Timestamp(i64),
    Bytes(Vec<u8>),
    Json(Value),
}

impl Cell {
    /// Coerce a parsed SQL literal (decoded to a [`serde_json::Value`] by `classify`)
    /// into a [`Cell`] of the target column type. The single place a relational value
    /// is type-checked on the write path. A `Value::Null` is allowed only when the
    /// column is `nullable` (callers pass `nullable`). Returns a precise error on a
    /// genuine type mismatch (e.g. text into an int column) so a bad INSERT is
    /// rejected, not silently corrupted.
    pub fn coerce(value: &Value, ty: ColumnType, nullable: bool) -> Result<Cell, String> {
        if value.is_null() {
            if nullable {
                return Ok(Cell::Null);
            }
            return Err("NULL value in a NOT NULL column".to_string());
        }
        let cell = match ty {
            ColumnType::Int | ColumnType::BigInt => match value.as_i64() {
                Some(i) => Cell::Int(i),
                None => return Err(format!("expected an integer, got `{value}`")),
            },
            ColumnType::Timestamp => match value.as_i64() {
                Some(i) => Cell::Timestamp(i),
                None => {
                    return Err(format!(
                        "expected a timestamp as integer microseconds, got `{value}`"
                    ))
                }
            },
            ColumnType::Float | ColumnType::Double => match value.as_f64() {
                Some(f) => Cell::Float(f),
                None => return Err(format!("expected a float, got `{value}`")),
            },
            ColumnType::Bool => match value.as_bool() {
                Some(b) => Cell::Bool(b),
                None => return Err(format!("expected a boolean, got `{value}`")),
            },
            ColumnType::Text => match value {
                Value::String(s) => Cell::Text(s.clone()),
                // A numeric/bool literal into a text column renders as its text form.
                Value::Number(_) | Value::Bool(_) => Cell::Text(value.to_string()),
                other => Cell::Text(other.to_string()),
            },
            ColumnType::Bytes => match value {
                // A string literal stores its UTF-8 bytes; a JSON array of small ints
                // stores those bytes (the `props`-style escape encoding).
                Value::String(s) => Cell::Bytes(s.clone().into_bytes()),
                Value::Array(items) => {
                    let mut bytes = Vec::with_capacity(items.len());
                    for it in items {
                        match it.as_u64() {
                            Some(n) if n <= 255 => bytes.push(n as u8),
                            _ => return Err(format!("invalid byte in bytes literal: `{it}`")),
                        }
                    }
                    Cell::Bytes(bytes)
                }
                other => return Err(format!("expected bytes, got `{other}`")),
            },
            ColumnType::Json => Cell::Json(value.clone()),
        };
        Ok(cell)
    }

    /// Render a cell back to a [`serde_json::Value`] — the inverse of [`Cell::coerce`]
    /// for the cases where it is exact (used by the WHERE-equality matcher so a
    /// `col = literal` predicate compares against the stored value).
    pub fn to_json(&self) -> Value {
        match self {
            Cell::Null => Value::Null,
            Cell::Int(i) | Cell::Timestamp(i) => Value::Number((*i).into()),
            Cell::Float(f) => serde_json::Number::from_f64(*f)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            Cell::Text(s) => Value::String(s.clone()),
            Cell::Bool(b) => Value::Bool(*b),
            Cell::Bytes(b) => Value::Array(b.iter().map(|x| Value::Number((*x).into())).collect()),
            Cell::Json(v) => v.clone(),
        }
    }
}
