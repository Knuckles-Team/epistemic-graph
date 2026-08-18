//! Column types, table schemas, and the typed row-cell value (CONCEPT:EG-KG.query.register-user-tables-alongside).
//!
//! These are the catalog's data model for an ARBITRARY user-defined relational
//! table — the relational sibling of the graph's schema-on-read `nodes` projection.
//! A user `CREATE TABLE` records a [`TableSchema`] (a name + an ordered list of typed
//! [`Column`]s) into the durable catalog; every stored row is a `Vec<Cell>` aligned
//! to that schema's column order. All three types are `serde`-serializable so the
//! redb table store persists them verbatim (MessagePack) and they round-trip across a
//! restart unchanged.

use std::collections::HashMap;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

/// The set of column types a user table column may declare (CONCEPT:EG-KG.query.register-user-tables-alongside). Chosen
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
    /// A pgvector `vector`/`vector(n)` dense float embedding (CONCEPT:EG-KG.query.vector-json-array-render), stored as
    /// a `Vec<f32>` (Arrow `List<Float32>`). `Some(n)` is the declared dimension (row
    /// length enforced on insert); `None` is an unconstrained-dimension vector.
    Vector(Option<usize>),
    /// RFC 4122 UUID (CONCEPT:EG-KG.query.table-schema-constraints/NE-002). Validated + normalized to canonical
    /// lowercase-hyphenated form on write, stored as `Cell::Text` (Arrow `Utf8` — same
    /// wire shape `Text` already uses, so no lossy drop). Distinct from `Text` purely
    /// for catalog/OID identity and format validation; a bad literal is rejected at
    /// the write path rather than silently accepted as opaque text.
    Uuid,
    /// `NUMERIC`/`DECIMAL[(p,s)]` exact fixed-point (CONCEPT:EG-KG.query.table-schema-constraints/NE-002).
    /// `Some((precision, scale))` is enforced on write (digit-count overflow and
    /// excess-scale are REJECTED rather than silently truncated/rounded); `None` is an
    /// unconstrained-precision numeric. Stored as `Cell::Float` (Arrow `Float64`,
    /// matching `Double`) — an inherited precision ceiling documented on
    /// [`Cell::coerce`]'s NUMERIC arm; the scale/precision CHECK is real even though the
    /// underlying storage is not arbitrary-precision decimal.
    Numeric(Option<(u32, u32)>),
    /// `TIMESTAMPTZ`/`TIMESTAMP WITH TIME ZONE` (CONCEPT:EG-KG.query.table-schema-constraints/NE-002) — unlike bare
    /// `Timestamp`, a string literal MUST carry an explicit UTC offset (`Z` or
    /// `±HH[:MM]`); a zone-less literal is rejected rather than silently treated as
    /// local time. Normalized to UTC and stored as `Cell::Timestamp` (i64 micros —
    /// same wire shape as `Timestamp`; the calendar/zone semantics are enforced only
    /// on the way IN).
    TimestampTz,
    /// `TEXT[]`/`UUID[]`/… (CONCEPT:EG-KG.query.table-schema-constraints/NE-002) — an array of a scalar element type
    /// (distinct from [`ColumnType::Vector`], which is a dense f32 embedding). Stored
    /// as `Cell::Json` (a genuine JSON array — Arrow `Utf8`, its canonical JSON text),
    /// so element-wise round-trip through SELECT is exact.
    Array(ArrayElemType),
}

/// The scalar element type of an [`ColumnType::Array`] column (CONCEPT:EG-KG.query.table-schema-constraints/NE-002).
/// Deliberately a small closed set (no nested arrays, no `Vector`/`Json`/`Bytes`
/// elements) so every element coerces through the SAME scalar [`Cell::coerce`] path a
/// bare column of that type would use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArrayElemType {
    Text,
    Uuid,
    Int,
    BigInt,
    Bool,
    Double,
}

impl ArrayElemType {
    /// Parse the SQL element-type spelling inside `type[]` / `_type` (case-insensitive).
    fn parse(name: &str) -> Result<ArrayElemType, String> {
        let base = name.trim().to_ascii_lowercase();
        let ty = match base.as_str() {
            "text" | "varchar" | "char" | "character" | "character varying" | "string" => {
                ArrayElemType::Text
            }
            "uuid" => ArrayElemType::Uuid,
            "int" | "int4" | "integer" | "smallint" | "int2" => ArrayElemType::Int,
            "bigint" | "int8" | "bigserial" | "long" => ArrayElemType::BigInt,
            "bool" | "boolean" => ArrayElemType::Bool,
            "double" | "float8" | "double precision" | "float" | "float4" | "real" => {
                ArrayElemType::Double
            }
            other => return Err(format!("unsupported array element type `{other}`")),
        };
        Ok(ty)
    }

    /// The scalar [`ColumnType`] each array element coerces through.
    pub fn as_column_type(self) -> ColumnType {
        match self {
            ArrayElemType::Text => ColumnType::Text,
            ArrayElemType::Uuid => ColumnType::Uuid,
            ArrayElemType::Int => ColumnType::Int,
            ArrayElemType::BigInt => ColumnType::BigInt,
            ArrayElemType::Bool => ColumnType::Bool,
            ArrayElemType::Double => ColumnType::Double,
        }
    }
}

impl ColumnType {
    /// Parse a SQL type name (case-insensitive, e.g. from `CREATE TABLE`) into a
    /// [`ColumnType`]. Accepts the common Postgres/standard spellings plus the
    /// engine's canonical names so `int4`/`integer`/`int` all land on `Int`, etc.
    /// Length/precision suffixes (`varchar(255)`, `numeric(10,2)`) are ignored — the
    /// base type is what the store needs. Returns `Err` for an unknown type.
    pub fn parse(name: &str) -> Result<ColumnType, String> {
        let trimmed = name.trim();
        // CONCEPT:EG-KG.query.table-schema-constraints/NE-002 — `<base>[]` (postgres array syntax) or the internal
        // `_<base>` spelling (`_text`, `_uuid`, …). Checked BEFORE the generic `(...)`
        // strip below since an array has no precision suffix of its own.
        if let Some(base) = trimmed.strip_suffix("[]") {
            return Ok(ColumnType::Array(ArrayElemType::parse(base)?));
        }
        if let Some(base) = trimmed.strip_prefix('_') {
            if let Ok(elem) = ArrayElemType::parse(base) {
                return Ok(ColumnType::Array(elem));
            }
        }
        // Strip any `(...)` precision/length suffix and whitespace, lowercase.
        let base = trimmed
            .split('(')
            .next()
            .unwrap_or(trimmed)
            .trim()
            .to_ascii_lowercase();
        // CONCEPT:EG-KG.query.vector-json-array-render — pgvector `vector`/`vector(n)`. The dimension is read from the
        // ORIGINAL spelling (the `(n)` suffix that the base-type strip above discards).
        if base == "vector" {
            return Ok(ColumnType::Vector(parse_vector_dim(trimmed)));
        }
        // CONCEPT:EG-KG.query.table-schema-constraints/NE-002 — `numeric`/`decimal[(p[,s])]`. The precision/scale is
        // read from the ORIGINAL spelling for the same reason `vector(n)` is.
        if base == "numeric" || base == "decimal" {
            return Ok(ColumnType::Numeric(parse_numeric_precision_scale(
                trimmed,
            )?));
        }
        let ty = match base.as_str() {
            "int" | "int4" | "integer" | "serial" | "smallint" | "int2" => ColumnType::Int,
            "bigint" | "int8" | "bigserial" | "long" => ColumnType::BigInt,
            "float" | "float4" | "real" => ColumnType::Float,
            "double" | "float8" | "double precision" => ColumnType::Double,
            "text" | "varchar" | "char" | "character" | "character varying" | "string" => {
                ColumnType::Text
            }
            "uuid" => ColumnType::Uuid,
            "bool" | "boolean" => ColumnType::Bool,
            "timestamptz" | "timestamp with time zone" => ColumnType::TimestampTz,
            "timestamp" | "datetime" | "timestamp without time zone" => ColumnType::Timestamp,
            "bytes" | "bytea" | "blob" | "binary" => ColumnType::Bytes,
            "json" | "jsonb" => ColumnType::Json,
            other => return Err(format!("unsupported column type `{other}`")),
        };
        Ok(ty)
    }
}

/// Extract `(precision[, scale])` from a `numeric`/`decimal` type spelling (CONCEPT:EG-KG.query.table-schema-constraints/NE-002).
/// `None` (bare `numeric`) is unconstrained precision. A malformed suffix, a
/// non-numeric precision/scale, or `scale > precision` is a hard error — a NUMERIC
/// column with a nonsensical declared shape is rejected at DDL time, not silently
/// truncated to something else.
fn parse_numeric_precision_scale(name: &str) -> Result<Option<(u32, u32)>, String> {
    let Some(start) = name.find('(') else {
        return Ok(None);
    };
    let Some(end_rel) = name[start + 1..].find(')') else {
        return Err(format!("unterminated precision in NUMERIC type `{name}`"));
    };
    let inner = name[start + 1..start + 1 + end_rel].trim();
    if inner.is_empty() {
        return Ok(None);
    }
    let mut parts = inner.split(',');
    let precision: u32 = parts
        .next()
        .unwrap_or_default()
        .trim()
        .parse()
        .map_err(|_| format!("invalid NUMERIC precision in `{name}`"))?;
    let scale: u32 = match parts.next() {
        Some(s) => s
            .trim()
            .parse()
            .map_err(|_| format!("invalid NUMERIC scale in `{name}`"))?,
        None => 0,
    };
    if parts.next().is_some() {
        return Err(format!("invalid NUMERIC type spelling `{name}`"));
    }
    if scale > precision {
        return Err(format!(
            "NUMERIC scale {scale} exceeds precision {precision} in `{name}`"
        ));
    }
    Ok(Some((precision, scale)))
}

/// Extract the declared dimension `n` from a `vector(n)` type spelling (CONCEPT:EG-KG.query.vector-json-array-render),
/// or `None` for a bare `vector`. A malformed/zero suffix yields `None` (unconstrained).
fn parse_vector_dim(name: &str) -> Option<usize> {
    let start = name.find('(')?;
    let end = name[start + 1..].find(')')?;
    name[start + 1..start + 1 + end]
        .trim()
        .parse::<usize>()
        .ok()
        .filter(|n| *n > 0)
}

/// A column-scoped comparison operator for a simple `CHECK (col OP literal)` constraint
/// (CONCEPT:EG-KG.query.register-each-user-table). Kept independent of the Cypher `CompareOp` so the relational store
/// has no cross-feature dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A simple single-column `CHECK` predicate (`CHECK (col OP literal)`) enforced on
/// insert/update (CONCEPT:EG-KG.query.register-each-user-table). Only the column-vs-literal comparison shape is
/// modeled; a complex CHECK expression is not stored (the classifier rejects it).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColCheck {
    pub op: CmpOp,
    pub value: Value,
}

impl ColCheck {
    /// Does `actual` satisfy the check? NULL passes (SQL CHECK is satisfied by NULL).
    pub fn holds(&self, actual: &Value) -> bool {
        if actual.is_null() {
            return true;
        }
        let ord = match (actual.as_f64(), self.value.as_f64()) {
            (Some(a), Some(b)) => a.partial_cmp(&b),
            _ => match (actual.as_str(), self.value.as_str()) {
                (Some(a), Some(b)) => Some(a.cmp(b)),
                _ => return matches!(self.op, CmpOp::Eq) && actual == &self.value,
            },
        };
        use std::cmp::Ordering::*;
        match (self.op, ord) {
            (CmpOp::Eq, _) => actual == &self.value,
            (CmpOp::Ne, _) => actual != &self.value,
            (CmpOp::Lt, Some(o)) => o == Less,
            (CmpOp::Le, Some(o)) => o != Greater,
            (CmpOp::Gt, Some(o)) => o == Greater,
            (CmpOp::Ge, Some(o)) => o != Less,
            (_, None) => false,
        }
    }
}

// ── table-level constraints (CONCEPT:EG-KG.query.table-schema-constraints/NE-001) ──────────────────────────

/// The `ON DELETE`/`ON UPDATE` referential action of a `FOREIGN KEY` (CONCEPT:EG-KG.query.table-schema-constraints/NE-001).
/// `SET DEFAULT` is deliberately NOT modeled — the store has no per-column DEFAULT
/// tracking wired to referential actions, so a `FOREIGN KEY … ON DELETE SET DEFAULT`
/// is REJECTED at DDL time (classify) rather than silently downgraded to one of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefAction {
    /// No enforcement at the constraint level beyond the base check (same as
    /// `RESTRICT` for this engine — there is no deferred-constraint machinery to
    /// distinguish them, matching Postgres's own non-deferred default behavior).
    NoAction,
    /// Reject the parent DELETE/UPDATE while a referencing child row exists.
    Restrict,
    /// Propagate the parent DELETE/UPDATE to every referencing child row.
    Cascade,
    /// Set the child row's FK columns to NULL (the columns must be nullable).
    SetNull,
}

/// A general boolean row predicate for a table-level `CHECK` (CONCEPT:EG-KG.query.table-schema-constraints/NE-001) —
/// strictly more expressive than the single-column [`ColCheck`]: `AND`/`OR` of
/// comparisons, `IN (...)`, `IS [NOT] NULL`, and a comparison between two columns of
/// the SAME row. Evaluated against a full row map (`column name -> JSON value`), not
/// just one column's value, so it can express cross-column invariants.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CheckExpr {
    /// `col OP literal`.
    Cmp { column: String, op: CmpOp, value: Value },
    /// `col_a OP col_b` — a comparison between two columns of the same row.
    ColCmp { left: String, op: CmpOp, right: String },
    /// `col [NOT] IN (v1, v2, …)`.
    In { column: String, values: Vec<Value>, negated: bool },
    /// `col IS [NOT] NULL`.
    IsNull { column: String, negated: bool },
    And(Box<CheckExpr>, Box<CheckExpr>),
    Or(Box<CheckExpr>, Box<CheckExpr>),
}

impl CheckExpr {
    /// Does `row` (a `column name -> JSON value` map) satisfy this expression? NULL
    /// participants pass a `Cmp`/`ColCmp` (SQL: a comparison against/between NULL is
    /// UNKNOWN, which a CHECK treats as satisfied) — mirrors [`ColCheck::holds`].
    pub fn holds(&self, row: &Map<String, Value>) -> bool {
        match self {
            CheckExpr::Cmp { column, op, value } => {
                let actual = row.get(column).cloned().unwrap_or(Value::Null);
                ColCheck {
                    op: *op,
                    value: value.clone(),
                }
                .holds(&actual)
            }
            CheckExpr::ColCmp { left, op, right } => {
                let a = row.get(left).cloned().unwrap_or(Value::Null);
                let b = row.get(right).cloned().unwrap_or(Value::Null);
                if a.is_null() || b.is_null() {
                    return true;
                }
                ColCheck {
                    op: *op,
                    value: b,
                }
                .holds(&a)
            }
            CheckExpr::In {
                column,
                values,
                negated,
            } => {
                let actual = row.get(column).cloned().unwrap_or(Value::Null);
                if actual.is_null() {
                    return true;
                }
                let contains = values.iter().any(|v| v == &actual);
                contains != *negated
            }
            CheckExpr::IsNull { column, negated } => {
                let actual = row.get(column).cloned().unwrap_or(Value::Null);
                actual.is_null() != *negated
            }
            CheckExpr::And(a, b) => a.holds(row) && b.holds(row),
            CheckExpr::Or(a, b) => a.holds(row) || b.holds(row),
        }
    }

    /// Every column name this expression references, for [`TableSchema::validate`]'s
    /// "the constraint references real columns" fail-closed check.
    fn referenced_columns<'a>(&'a self, out: &mut Vec<&'a str>) {
        match self {
            CheckExpr::Cmp { column, .. } | CheckExpr::In { column, .. } | CheckExpr::IsNull { column, .. } => {
                out.push(column);
            }
            CheckExpr::ColCmp { left, right, .. } => {
                out.push(left);
                out.push(right);
            }
            CheckExpr::And(a, b) | CheckExpr::Or(a, b) => {
                a.referenced_columns(out);
                b.referenced_columns(out);
            }
        }
    }
}

/// A table-level constraint (CONCEPT:EG-KG.query.table-schema-constraints/NE-001) — the composite/cross-column
/// counterpart to [`Column`]'s per-column flags. An optional `name` is the
/// user-supplied `CONSTRAINT <name>`; `None` means the store synthesizes a
/// Postgres-style name (`TableSchema::synth_constraint_name`) for `DROP CONSTRAINT`
/// matching.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TableConstraint {
    /// `PRIMARY KEY (a, b, …)`. At most ONE primary key (column-level OR table-level)
    /// may exist per table — enforced by [`TableSchema::validate`].
    PrimaryKey { name: Option<String>, columns: Vec<String> },
    /// `UNIQUE (a, b, …)`.
    Unique { name: Option<String>, columns: Vec<String> },
    /// `FOREIGN KEY (a, b, …) REFERENCES ref_table(x, y, …) [ON DELETE …] [ON UPDATE …]`.
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
        on_delete: RefAction,
        on_update: RefAction,
    },
    /// A general `CHECK (<expr>)`.
    Check { name: Option<String>, expr: CheckExpr },
}

impl TableConstraint {
    pub fn name(&self) -> Option<&str> {
        match self {
            TableConstraint::PrimaryKey { name, .. }
            | TableConstraint::Unique { name, .. }
            | TableConstraint::ForeignKey { name, .. }
            | TableConstraint::Check { name, .. } => name.as_deref(),
        }
    }
}

/// One column of a user table: its name, declared type, NULL-ability, primary-key /
/// uniqueness participation, an optional column DEFAULT, SERIAL auto-increment, and an
/// optional simple CHECK (CONCEPT:EG-KG.query.register-user-tables-alongside + constraints CONCEPT:EG-KG.query.register-each-user-table).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub ty: ColumnType,
    pub nullable: bool,
    pub primary_key: bool,
    /// `UNIQUE` (or `PRIMARY KEY`) — enforced on insert/update (CONCEPT:EG-KG.query.register-each-user-table).
    pub unique: bool,
    /// `SERIAL`/`BIGSERIAL` (or `DEFAULT nextval(...)`) — auto-assigned from the
    /// per-table sequence when not supplied (CONCEPT:EG-KG.query.register-each-user-table).
    pub serial: bool,
    /// Column `DEFAULT <literal>` value, used when a row omits the column.
    pub default: Option<Value>,
    /// Optional simple `CHECK (col OP literal)` enforced on the write path.
    pub check: Option<ColCheck>,
}

impl Column {
    /// A plain column with no constraints beyond name/type/nullability/PK.
    pub fn new(name: impl Into<String>, ty: ColumnType, nullable: bool, primary_key: bool) -> Self {
        Self {
            name: name.into(),
            ty,
            nullable,
            primary_key,
            unique: primary_key,
            serial: false,
            default: None,
            check: None,
        }
    }

    /// Whether this column enforces uniqueness (PK or UNIQUE).
    pub fn is_unique(&self) -> bool {
        self.unique || self.primary_key
    }
}

/// A user table's full schema: its name and ordered columns (CONCEPT:EG-KG.query.register-user-tables-alongside). The
/// column ORDER is canonical — every stored row's `Vec<Cell>` is aligned to it, and
/// a SELECT projects columns in this order unless the query names them explicitly.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableSchema {
    pub name: String,
    columns: Vec<Column>,
    /// Table-level constraints (CONCEPT:EG-KG.query.table-schema-constraints/NE-001) — composite PK/UNIQUE,
    /// FOREIGN KEY, and general CHECK. `#[serde(default)]` so a schema PERSISTED
    /// before this field existed still deserializes (round-trips to an empty vec).
    #[serde(default)]
    constraints: Vec<TableConstraint>,
    /// Derived name directory. It is rebuilt after deserialization and deliberately
    /// excluded from the durable schema so persistence has one canonical source of
    /// truth: the ordered `columns` vector.
    #[serde(skip)]
    column_offsets: OnceLock<Result<HashMap<String, usize>, String>>,
}

impl Clone for TableSchema {
    fn clone(&self) -> Self {
        // A clone is intentionally cold. Copying a populated directory would make
        // cache state observable through clone/equality and duplicates derived data.
        Self::new(self.name.clone(), self.columns.clone()).with_constraints(self.constraints.clone())
    }
}

impl PartialEq for TableSchema {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.columns == other.columns && self.constraints == other.constraints
    }
}

impl TableSchema {
    pub fn new(name: impl Into<String>, columns: Vec<Column>) -> Self {
        Self {
            name: name.into(),
            columns,
            constraints: Vec::new(),
            column_offsets: OnceLock::new(),
        }
    }

    /// Attach table-level constraints (CONCEPT:EG-KG.query.table-schema-constraints/NE-001) to an already-built
    /// schema — a builder step so [`TableSchema::new`]'s signature (and every
    /// existing 2-arg caller) stays unchanged.
    pub fn with_constraints(mut self, constraints: Vec<TableConstraint>) -> Self {
        self.constraints = constraints;
        self.column_offsets.take();
        self
    }

    /// The table's table-level constraints, in declaration order.
    pub fn constraints(&self) -> &[TableConstraint] {
        &self.constraints
    }

    /// Append one table-level constraint (`ALTER TABLE … ADD CONSTRAINT`, CONCEPT:EG-KG.query.table-schema-constraints/NE-001).
    pub fn push_constraint(&mut self, constraint: TableConstraint) {
        self.constraints.push(constraint);
        self.column_offsets.take();
    }

    /// Drop the table-level constraint named `name` (explicit or synthesized via
    /// [`TableSchema::synth_constraint_name`]). Returns whether one was removed.
    pub fn remove_constraint_named(&mut self, name: &str) -> bool {
        let before = self.constraints.len();
        let table = self.name.clone();
        self.constraints
            .retain(|c| Self::constraint_display_name(&table, c) != name);
        let removed = self.constraints.len() != before;
        if removed {
            self.column_offsets.take();
        }
        removed
    }

    /// The name a `DROP CONSTRAINT` matches against: the user-supplied name if any,
    /// else a Postgres-style synthesized name (CONCEPT:EG-KG.query.table-schema-constraints/NE-001) — mirrors the
    /// existing column-flag synthesis in `store::drop_constraint_in`
    /// (`<table>_pkey` / `<table>_<col>_key` / `<table>_<col>_check`).
    pub fn constraint_display_name(table: &str, c: &TableConstraint) -> String {
        if let Some(n) = c.name() {
            return n.to_string();
        }
        match c {
            TableConstraint::PrimaryKey { .. } => format!("{table}_pkey"),
            TableConstraint::Unique { columns, .. } => format!("{table}_{}_key", columns.join("_")),
            TableConstraint::ForeignKey { columns, .. } => format!("{table}_{}_fkey", columns.join("_")),
            TableConstraint::Check { .. } => format!("{table}_check"),
        }
    }

    /// Validate durable schema invariants and initialize the derived directory.
    /// Invalid persisted schemas fail closed instead of making duplicate names
    /// resolve according to vector order.
    pub fn validate(&self) -> Result<(), String> {
        validate_schema_name(&self.name, "table")?;
        if self.columns.is_empty() {
            return Err(format!(
                "table `{}` must declare at least one column",
                self.name
            ));
        }
        self.column_offsets()
            .map(|_| ())
            .map_err(|error| error.to_string())?;
        self.validate_constraints()
    }

    /// CONCEPT:EG-KG.query.table-schema-constraints/NE-001 — structural constraint validation local to THIS schema:
    /// referenced columns exist, at most one primary key (column-level flag OR
    /// table-level `PrimaryKey`), no empty column lists, and (for `ForeignKey`) the
    /// local/referenced column counts match. Cross-table FK existence (the referenced
    /// table/columns are real, and are themselves unique) is checked by the store at
    /// `CREATE TABLE`/`ADD CONSTRAINT` time, where the catalog is reachable.
    fn validate_constraints(&self) -> Result<(), String> {
        let mut pk_count = self.columns.iter().filter(|c| c.primary_key).count();
        for c in &self.constraints {
            match c {
                TableConstraint::PrimaryKey { columns, .. } => {
                    pk_count += 1;
                    self.validate_constraint_columns(columns, "PRIMARY KEY")?;
                }
                TableConstraint::Unique { columns, .. } => {
                    self.validate_constraint_columns(columns, "UNIQUE")?;
                }
                TableConstraint::ForeignKey {
                    columns,
                    ref_columns,
                    ..
                } => {
                    self.validate_constraint_columns(columns, "FOREIGN KEY")?;
                    if ref_columns.is_empty() {
                        return Err(format!(
                            "table `{}`: FOREIGN KEY REFERENCES must name at least one column",
                            self.name
                        ));
                    }
                    if columns.len() != ref_columns.len() {
                        return Err(format!(
                            "table `{}`: FOREIGN KEY column count ({}) does not match REFERENCES column count ({})",
                            self.name,
                            columns.len(),
                            ref_columns.len()
                        ));
                    }
                }
                TableConstraint::Check { expr, .. } => {
                    let mut referenced = Vec::new();
                    expr.referenced_columns(&mut referenced);
                    self.validate_constraint_columns(&referenced, "CHECK")?;
                }
            }
        }
        if pk_count > 1 {
            return Err(format!(
                "table `{}` declares more than one PRIMARY KEY",
                self.name
            ));
        }
        Ok(())
    }

    fn validate_constraint_columns(&self, columns: &[impl AsRef<str>], kind: &str) -> Result<(), String> {
        if columns.is_empty() {
            return Err(format!(
                "table `{}`: {kind} must name at least one column",
                self.name
            ));
        }
        for name in columns {
            let name = name.as_ref();
            if self.columns.iter().all(|c| c.name != name) {
                return Err(format!(
                    "table `{}`: {kind} references unknown column `{name}`",
                    self.name
                ));
            }
        }
        Ok(())
    }

    /// Ordered columns. Rows remain positionally aligned with this slice.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Mutably borrow the ordered columns, invalidating the derived name directory
    /// before any possible name/shape change.
    pub fn columns_mut(&mut self) -> &mut Vec<Column> {
        self.column_offsets.take();
        &mut self.columns
    }

    /// The position of `column` in the schema's column order, if present.
    /// The first lookup builds the directory in O(W); warm lookups are expected O(1).
    pub fn column_index(&self, column: &str) -> Option<usize> {
        self.column_offsets().ok()?.get(column).copied()
    }

    /// The column with the given name, if present.
    pub fn column(&self, column: &str) -> Option<&Column> {
        self.column_index(column)
            .and_then(|offset| self.columns.get(offset))
    }

    /// SHA-256 hex digest over the canonical `(name, columns)` shape (GOC-10:
    /// `eg_types::lake_catalog::TableSchemaVersionV1::schema_digest`). Two schemas
    /// that would `assert_eq!` via [`PartialEq`] always produce the SAME digest;
    /// any observable difference (a renamed/reordered/retyped/reconstrained
    /// column, or a renamed table) always produces a DIFFERENT one. Digests over
    /// deterministic MessagePack (`rmp_serde::to_vec_named`, the SAME encoding the
    /// durable store already persists a schema as) rather than `{:?}`, whose
    /// formatting is not a stability contract.
    pub fn schema_digest(&self) -> Result<String, String> {
        let encoded = rmp_serde::to_vec_named(&(&self.name, &self.columns, &self.constraints))
            .map_err(|error| format!("could not encode schema for digest: {error}"))?;
        let mut hasher = Sha256::new();
        hasher.update(b"epistemic-graph/table-schema-digest\0");
        hasher.update(&encoded);
        Ok(hex::encode(hasher.finalize()))
    }

    fn column_offsets(&self) -> Result<&HashMap<String, usize>, &String> {
        self.column_offsets
            .get_or_init(|| {
                let mut offsets = HashMap::with_capacity(self.columns.len());
                for (offset, column) in self.columns.iter().enumerate() {
                    validate_schema_name(&column.name, "column")?;
                    if offsets.insert(column.name.clone(), offset).is_some() {
                        return Err(format!(
                            "table `{}` declares duplicate column `{}`",
                            self.name, column.name
                        ));
                    }
                }
                Ok(offsets)
            })
            .as_ref()
    }
}

fn validate_schema_name(value: &str, kind: &str) -> Result<(), String> {
    if value.is_empty() || value.contains('\0') {
        return Err(format!("{kind} name is empty or contains NUL"));
    }
    Ok(())
}

#[cfg(test)]
mod table_schema_tests {
    use super::*;

    fn column(name: &str) -> Column {
        Column::new(name, ColumnType::Text, true, false)
    }

    #[test]
    fn column_directory_is_lazy_and_warm_lookups_are_direct() {
        let schema = TableSchema::new("events", vec![column("id"), column("payload")]);
        assert!(schema.column_offsets.get().is_none());
        assert_eq!(schema.column_index("payload"), Some(1));
        assert!(schema.column_offsets.get().is_some());
        assert_eq!(
            schema.column("id").map(|value| value.name.as_str()),
            Some("id")
        );
        assert_eq!(schema.column_index("missing"), None);
    }

    #[test]
    fn serialized_schema_excludes_and_rebuilds_the_directory() {
        let schema = TableSchema::new("events", vec![column("id"), column("payload")]);
        assert_eq!(schema.column_index("payload"), Some(1));

        let encoded = serde_json::to_value(&schema).unwrap();
        assert!(encoded.get("column_offsets").is_none());
        let restored: TableSchema = serde_json::from_value(encoded).unwrap();
        assert_eq!(restored, schema);
        assert!(restored.column_offsets.get().is_none());
        assert_eq!(restored.column_index("payload"), Some(1));
    }

    #[test]
    fn clone_preserves_only_canonical_schema_and_rebuilds_lazily() {
        let schema = TableSchema::new("events", vec![column("id"), column("payload")]);
        assert_eq!(schema.column_index("payload"), Some(1));
        let cloned = schema.clone();
        assert_eq!(cloned, schema);
        assert!(cloned.column_offsets.get().is_none());
        assert_eq!(cloned.column_index("payload"), Some(1));
    }

    // ── GOC-10: schema_digest ────────────────────────────────────────────

    #[test]
    fn schema_digest_is_deterministic_and_well_formed() {
        let schema = TableSchema::new("events", vec![column("id"), column("payload")]);
        let first = schema.schema_digest().unwrap();
        let second = schema.schema_digest().unwrap();
        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
        assert!(first.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }

    #[test]
    fn schema_digest_changes_with_any_observable_difference() {
        let base = TableSchema::new("events", vec![column("id"), column("payload")]);
        let base_digest = base.schema_digest().unwrap();

        // Known-bad-if-unchanged inputs: each of these must NOT collide with the
        // base digest, or two distinguishable schemas would be indistinguishable
        // to a `TableSchemaVersionV1` consumer.
        let renamed_table = TableSchema::new("other_events", vec![column("id"), column("payload")]);
        assert_ne!(renamed_table.schema_digest().unwrap(), base_digest);

        let dropped_column = TableSchema::new("events", vec![column("id")]);
        assert_ne!(dropped_column.schema_digest().unwrap(), base_digest);

        let reordered = TableSchema::new("events", vec![column("payload"), column("id")]);
        assert_ne!(reordered.schema_digest().unwrap(), base_digest);

        let mut retyped_columns = vec![column("id"), column("payload")];
        retyped_columns[1].ty = ColumnType::Int;
        let retyped = TableSchema::new("events", retyped_columns);
        assert_ne!(retyped.schema_digest().unwrap(), base_digest);
    }

    #[test]
    fn schema_digest_matches_for_equal_schemas() {
        let a = TableSchema::new("events", vec![column("id"), column("payload")]);
        let b = TableSchema::new("events", vec![column("id"), column("payload")]);
        assert_eq!(a, b);
        assert_eq!(a.schema_digest().unwrap(), b.schema_digest().unwrap());
    }

    #[test]
    fn mutation_invalidates_the_directory_and_validation_rejects_bad_names() {
        let mut schema = TableSchema::new("events", vec![column("id"), column("payload")]);
        assert_eq!(schema.column_index("id"), Some(0));
        schema.columns_mut()[0].name = "event_id".to_string();
        assert!(schema.column_offsets.get().is_none());
        assert_eq!(schema.column_index("event_id"), Some(0));
        assert_eq!(schema.column_index("id"), None);

        let duplicate = TableSchema::new("events", vec![column("id"), column("id")]);
        assert!(duplicate
            .validate()
            .unwrap_err()
            .contains("duplicate column"));
        assert_eq!(duplicate.column_index("id"), None);

        let malformed = TableSchema::new("events", vec![column("")]);
        assert!(malformed.validate().unwrap_err().contains("column name"));
    }

    // ── table-level constraints (CONCEPT:EG-KG.query.table-schema-constraints/NE-001) ────────────────────────

    #[test]
    fn old_serialized_schema_without_constraints_field_still_deserializes() {
        // A schema PERSISTED before `constraints` existed — no such key at all. Given
        // `#[serde(deny_unknown_fields)]`, the only way this round-trips is
        // `#[serde(default)]` on the new field.
        let old_json = serde_json::json!({
            "name": "events",
            "columns": [
                {
                    "name": "id",
                    "ty": "Text",
                    "nullable": true,
                    "primary_key": false,
                    "unique": false,
                    "serial": false,
                    "default": null,
                    "check": null
                }
            ]
        });
        let restored: TableSchema = serde_json::from_value(old_json).expect("old schema deserializes");
        assert!(restored.constraints().is_empty());
        assert!(restored.validate().is_ok());
    }

    #[test]
    fn composite_primary_key_forces_not_null_and_rejects_duplicate_pk_declarations() {
        let schema = TableSchema::new(
            "order_items",
            vec![column("order_id"), column("product_id")],
        )
        .with_constraints(vec![TableConstraint::PrimaryKey {
            name: None,
            columns: vec!["order_id".into(), "product_id".into()],
        }]);
        assert!(schema.validate().is_ok());

        // A second PK declaration (table-level PLUS a column-level `primary_key` flag)
        // is rejected — at most one PK per table.
        let mut cols = vec![column("order_id"), column("product_id")];
        cols[0].primary_key = true;
        let double_pk = TableSchema::new("order_items", cols).with_constraints(vec![
            TableConstraint::PrimaryKey {
                name: None,
                columns: vec!["product_id".into()],
            },
        ]);
        assert!(double_pk
            .validate()
            .unwrap_err()
            .contains("more than one PRIMARY KEY"));
    }

    #[test]
    fn constraint_referencing_unknown_column_is_rejected() {
        let schema = TableSchema::new("t", vec![column("a")]).with_constraints(vec![
            TableConstraint::Unique {
                name: None,
                columns: vec!["missing".into()],
            },
        ]);
        assert!(schema
            .validate()
            .unwrap_err()
            .contains("unknown column"));
    }

    #[test]
    fn constraint_display_name_synthesizes_postgres_style_names() {
        let named = TableConstraint::Unique {
            name: Some("my_uk".into()),
            columns: vec!["a".into()],
        };
        assert_eq!(TableSchema::constraint_display_name("t", &named), "my_uk");
        let unnamed = TableConstraint::Unique {
            name: None,
            columns: vec!["a".into(), "b".into()],
        };
        assert_eq!(
            TableSchema::constraint_display_name("t", &unnamed),
            "t_a_b_key"
        );
        let pk = TableConstraint::PrimaryKey {
            name: None,
            columns: vec!["a".into()],
        };
        assert_eq!(TableSchema::constraint_display_name("t", &pk), "t_pkey");
        let fk = TableConstraint::ForeignKey {
            name: None,
            columns: vec!["a".into()],
            ref_table: "other".into(),
            ref_columns: vec!["id".into()],
            on_delete: RefAction::NoAction,
            on_update: RefAction::NoAction,
        };
        assert_eq!(TableSchema::constraint_display_name("t", &fk), "t_a_fkey");
    }

    #[test]
    fn schema_digest_changes_when_only_constraints_differ() {
        let base = TableSchema::new("t", vec![column("a"), column("b")]);
        let with_constraint = base.clone().with_constraints(vec![TableConstraint::Unique {
            name: None,
            columns: vec!["a".into(), "b".into()],
        }]);
        assert_ne!(
            base.schema_digest().unwrap(),
            with_constraint.schema_digest().unwrap()
        );
    }

    // ── general CHECK expressions (CONCEPT:EG-KG.query.table-schema-constraints/NE-001) ──────────────────────

    fn row(pairs: &[(&str, Value)]) -> Map<String, Value> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    #[test]
    fn check_expr_and_or_in_isnull_colcmp_all_evaluate() {
        // (lo <= hi) AND status IN ('a','b')
        let expr = CheckExpr::And(
            Box::new(CheckExpr::ColCmp {
                left: "lo".into(),
                op: CmpOp::Le,
                right: "hi".into(),
            }),
            Box::new(CheckExpr::In {
                column: "status".into(),
                values: vec![Value::String("a".into()), Value::String("b".into())],
                negated: false,
            }),
        );
        assert!(expr.holds(&row(&[
            ("lo", Value::Number(1.into())),
            ("hi", Value::Number(2.into())),
            ("status", Value::String("a".into())),
        ])));
        assert!(!expr.holds(&row(&[
            ("lo", Value::Number(5.into())),
            ("hi", Value::Number(2.into())),
            ("status", Value::String("a".into())),
        ])));
        assert!(!expr.holds(&row(&[
            ("lo", Value::Number(1.into())),
            ("hi", Value::Number(2.into())),
            ("status", Value::String("z".into())),
        ])));

        // OR + IS NULL: status IS NULL OR status IN ('a')
        let or_expr = CheckExpr::Or(
            Box::new(CheckExpr::IsNull {
                column: "status".into(),
                negated: false,
            }),
            Box::new(CheckExpr::In {
                column: "status".into(),
                values: vec![Value::String("a".into())],
                negated: false,
            }),
        );
        assert!(or_expr.holds(&row(&[("status", Value::Null)])));
        assert!(or_expr.holds(&row(&[("status", Value::String("a".into()))])));
        assert!(!or_expr.holds(&row(&[("status", Value::String("z".into()))])));

        // IS NOT NULL negation.
        let not_null = CheckExpr::IsNull {
            column: "status".into(),
            negated: true,
        };
        assert!(not_null.holds(&row(&[("status", Value::String("a".into()))])));
        assert!(!not_null.holds(&row(&[("status", Value::Null)])));

        // NULL participant in a ColCmp/In is treated as UNKNOWN ⇒ satisfied (mirrors ColCheck).
        let colcmp_null = CheckExpr::ColCmp {
            left: "lo".into(),
            op: CmpOp::Lt,
            right: "hi".into(),
        };
        assert!(colcmp_null.holds(&row(&[("lo", Value::Null), ("hi", Value::Number(1.into()))])));
    }

    // ── NE-002: UUID / NUMERIC / TIMESTAMPTZ / ARRAY ────────────────────────────────

    #[test]
    fn column_type_parse_recognizes_new_postgres_spellings() {
        assert_eq!(ColumnType::parse("uuid").unwrap(), ColumnType::Uuid);
        assert_eq!(
            ColumnType::parse("numeric(10,2)").unwrap(),
            ColumnType::Numeric(Some((10, 2)))
        );
        assert_eq!(
            ColumnType::parse("decimal(5)").unwrap(),
            ColumnType::Numeric(Some((5, 0)))
        );
        assert_eq!(ColumnType::parse("numeric").unwrap(), ColumnType::Numeric(None));
        assert_eq!(
            ColumnType::parse("timestamptz").unwrap(),
            ColumnType::TimestampTz
        );
        assert_eq!(
            ColumnType::parse("timestamp with time zone").unwrap(),
            ColumnType::TimestampTz
        );
        // Bare `timestamp` stays the zone-less type.
        assert_eq!(ColumnType::parse("timestamp").unwrap(), ColumnType::Timestamp);
        assert_eq!(
            ColumnType::parse("text[]").unwrap(),
            ColumnType::Array(ArrayElemType::Text)
        );
        assert_eq!(
            ColumnType::parse("_uuid").unwrap(),
            ColumnType::Array(ArrayElemType::Uuid)
        );
        assert!(ColumnType::parse("numeric(3,10)").is_err(), "scale > precision rejected");
    }

    #[test]
    fn uuid_round_trips_and_normalizes_case_and_hyphens() {
        let cell = Cell::coerce(
            &Value::String("A1A2A3A4-B1B2-C1C2-D1D2-E1E2E3E4E5E6".into()),
            ColumnType::Uuid,
            false,
        )
        .unwrap();
        assert_eq!(
            cell,
            Cell::Text("a1a2a3a4-b1b2-c1c2-d1d2-e1e2e3e4e5e6".into())
        );
        // A bare 32-hex-digit form normalizes to the SAME hyphenated canonical form.
        let cell2 = Cell::coerce(
            &Value::String("a1a2a3a4b1b2c1c2d1d2e1e2e3e4e5e6".into()),
            ColumnType::Uuid,
            false,
        )
        .unwrap();
        assert_eq!(cell2, cell);
        assert!(Cell::coerce(&Value::String("not-a-uuid".into()), ColumnType::Uuid, false).is_err());
    }

    #[test]
    fn numeric_enforces_precision_scale_and_rejects_overflow() {
        let cell = Cell::coerce(
            &Value::String("123.4".into()),
            ColumnType::Numeric(Some((5, 2))),
            false,
        )
        .unwrap();
        assert_eq!(cell, Cell::Float(123.40));
        // Too many fractional digits ⇒ rejected (never silently rounded away).
        assert!(Cell::coerce(
            &Value::String("1.234".into()),
            ColumnType::Numeric(Some((5, 2))),
            false
        )
        .is_err());
        // Too many integer digits ⇒ rejected.
        assert!(Cell::coerce(
            &Value::String("1234.5".into()),
            ColumnType::Numeric(Some((5, 2))),
            false
        )
        .is_err());
    }

    #[test]
    fn timestamptz_requires_explicit_offset_and_normalizes_to_utc() {
        // A zone-less literal is REJECTED, not silently treated as local time.
        assert!(Cell::coerce(
            &Value::String("2024-01-01T00:00:00".into()),
            ColumnType::TimestampTz,
            false
        )
        .is_err());
        let utc = Cell::coerce(
            &Value::String("2024-01-01T00:00:00Z".into()),
            ColumnType::TimestampTz,
            false,
        )
        .unwrap();
        // 2024-01-01T00:00:00Z epoch micros.
        assert_eq!(utc, Cell::Timestamp(1_704_067_200_000_000));
        // A +05:00 literal normalizes 5 hours EARLIER than the same wall-clock UTC.
        let offset = Cell::coerce(
            &Value::String("2024-01-01T05:00:00+05:00".into()),
            ColumnType::TimestampTz,
            false,
        )
        .unwrap();
        assert_eq!(offset, utc);
    }

    #[test]
    fn array_round_trips_and_validates_each_element() {
        let cell = Cell::coerce(
            &Value::Array(vec![Value::String("a".into()), Value::String("b".into())]),
            ColumnType::Array(ArrayElemType::Text),
            false,
        )
        .unwrap();
        assert_eq!(
            cell,
            Cell::Json(Value::Array(vec![
                Value::String("a".into()),
                Value::String("b".into())
            ]))
        );
        // A bad UUID element is rejected, not silently stored.
        assert!(Cell::coerce(
            &Value::Array(vec![Value::String("not-a-uuid".into())]),
            ColumnType::Array(ArrayElemType::Uuid),
            false
        )
        .is_err());
        // The postgres TEXT-protocol `{a,b}` spelling is also accepted.
        let from_text = Cell::coerce(
            &Value::String("{a,b}".into()),
            ColumnType::Array(ArrayElemType::Text),
            false,
        )
        .unwrap();
        assert_eq!(from_text, cell);
    }
}

/// One typed cell value of a stored row (CONCEPT:EG-KG.query.register-user-tables-alongside). A row is a `Vec<Cell>`
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
    /// A dense float embedding (CONCEPT:EG-KG.query.vector-json-array-render), stored as `Vec<f32>`.
    Vector(Vec<f32>),
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
            // CONCEPT:EG-KG.query.vector-json-array-render — a `vector`/`vector(n)` accepts either a JSON array of
            // numbers (`[1,2,3]`) or the pgvector text literal `'[1,2,3]'`; both decode
            // to a `Vec<f32>`. When the column declares a dimension, the row length is
            // enforced so a mis-shaped embedding is rejected, not silently stored.
            ColumnType::Vector(dim) => {
                let floats = parse_vector_value(value)?;
                if let Some(n) = dim {
                    if floats.len() != n {
                        return Err(format!(
                            "vector has {} dimensions, column declares {n}",
                            floats.len()
                        ));
                    }
                }
                Cell::Vector(floats)
            }
            // CONCEPT:EG-KG.query.table-schema-constraints/NE-002 — a UUID literal is validated (32 hex digits,
            // optionally 8-4-4-4-12 hyphenated) and normalized to canonical lowercase.
            ColumnType::Uuid => match value {
                Value::String(s) => Cell::Text(normalize_uuid(s)?),
                other => return Err(format!("expected a UUID string, got `{other}`")),
            },
            // A TIMESTAMPTZ literal is either already-UTC integer microseconds, or an
            // ISO-8601 string carrying an EXPLICIT offset (`Z`/`+HH:MM`/`-HHMM`); a
            // zone-less string is rejected rather than silently treated as local time.
            ColumnType::TimestampTz => match value {
                Value::Number(_) => match value.as_i64() {
                    Some(i) => Cell::Timestamp(i),
                    None => {
                        return Err(format!(
                            "expected a timestamptz as integer microseconds, got `{value}`"
                        ))
                    }
                },
                Value::String(s) => Cell::Timestamp(parse_timestamptz(s)?),
                other => return Err(format!("expected a timestamptz, got `{other}`")),
            },
            // CONCEPT:EG-KG.query.table-schema-constraints/NE-002 — a NUMERIC/DECIMAL(p,s) literal is validated
            // (digit-count overflow / excess scale REJECTED, never silently truncated)
            // and rounded to the declared scale; storage is `Cell::Float` (documented
            // precision ceiling on the type's doc comment).
            ColumnType::Numeric(precision_scale) => {
                let raw = numeric_literal_text(value)?;
                let canon = normalize_numeric_literal(&raw, precision_scale)?;
                let f: f64 = canon
                    .parse()
                    .map_err(|_| format!("invalid NUMERIC literal `{raw}`"))?;
                Cell::Float(f)
            }
            // CONCEPT:EG-KG.query.table-schema-constraints/NE-002 — an array literal (a JSON array, or the postgres
            // text form `{a,b,c}`); every element is coerced through the scalar
            // element type's OWN `Cell::coerce` (so e.g. a `uuid[]` element is
            // format-validated + normalized exactly like a bare `uuid` column) and the
            // canonical per-element JSON is what is stored — a genuine JSON array, so
            // round-trip through SELECT is exact.
            ColumnType::Array(elem) => {
                let items = match value {
                    Value::Array(items) => items.clone(),
                    Value::String(s) => parse_pg_array_text(s)?,
                    other => return Err(format!("expected an array literal, got `{other}`")),
                };
                let mut out = Vec::with_capacity(items.len());
                for item in &items {
                    let cell = Cell::coerce(item, elem.as_column_type(), true)?;
                    out.push(cell.to_json());
                }
                Cell::Json(Value::Array(out))
            }
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
            // CONCEPT:EG-KG.query.vector-json-array-render — a vector renders back as a JSON array of numbers; the
            // pgwire shim re-serializes that array to the pgvector text form `[1,2,3]`.
            Cell::Vector(v) => Value::Array(
                v.iter()
                    .map(|f| {
                        serde_json::Number::from_f64(*f as f64)
                            .map(Value::Number)
                            .unwrap_or(Value::Null)
                    })
                    .collect(),
            ),
        }
    }
}

/// Parse a JSON value into a dense `Vec<f32>` for a vector column (CONCEPT:EG-KG.query.vector-json-array-render):
/// a JSON array of numbers, or the pgvector text literal `[1,2,3]` (an array or a
/// bracketed/comma string). Rejects a non-numeric element with a precise error.
fn parse_vector_value(value: &Value) -> Result<Vec<f32>, String> {
    match value {
        Value::Array(items) => items
            .iter()
            .map(|it| {
                it.as_f64()
                    .map(|f| f as f32)
                    .ok_or_else(|| format!("vector element is not a number: `{it}`"))
            })
            .collect(),
        Value::String(s) => parse_vector_text(s),
        other => Err(format!(
            "expected a vector (array or '[...]' text), got `{other}`"
        )),
    }
}

/// Parse a pgvector text literal `[1,2,3]` (brackets optional, comma/whitespace
/// separated) into a `Vec<f32>` (CONCEPT:EG-KG.query.vector-json-array-render).
pub(crate) fn parse_vector_text(s: &str) -> Result<Vec<f32>, String> {
    let inner = s
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .trim();
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    inner
        .split(',')
        .map(|tok| {
            tok.trim()
                .parse::<f32>()
                .map_err(|_| format!("invalid vector element `{}`", tok.trim()))
        })
        .collect()
}

// ── UUID / NUMERIC / TIMESTAMPTZ / ARRAY literal decoding (CONCEPT:EG-KG.query.table-schema-constraints/NE-002) ──

/// Validate + canonicalize a UUID literal (CONCEPT:EG-KG.query.table-schema-constraints/NE-002): 32 hex digits,
/// either bare or in the standard `8-4-4-4-12` hyphenated grouping (case-insensitive
/// either way). Returns the canonical lowercase hyphenated form. Any other shape is a
/// hard error — never silently accepted as opaque text.
fn normalize_uuid(s: &str) -> Result<String, String> {
    let raw = s.trim();
    if raw.contains('-') {
        let parts: Vec<&str> = raw.split('-').collect();
        let want = [8usize, 4, 4, 4, 12];
        if parts.len() != 5 || parts.iter().zip(want).any(|(p, w)| p.len() != w) {
            return Err(format!("invalid UUID literal `{s}`"));
        }
    }
    let hex: String = raw.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(format!("invalid UUID literal `{s}`"));
    }
    let hex = hex.to_ascii_lowercase();
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    ))
}

/// Render a JSON value into the decimal TEXT a NUMERIC literal parser accepts (CONCEPT:EG-KG.query.table-schema-constraints/NE-002).
/// A `Value::String` passes through verbatim (the lossless path — a caller that wants
/// exact NUMERIC precision beyond what a JSON number can hold should supply a string
/// literal). A `Value::Number` renders via its own `Display`, which is only as precise
/// as whatever precision already survived the caller's own JSON encoding.
fn numeric_literal_text(value: &Value) -> Result<String, String> {
    match value {
        Value::String(s) => Ok(s.trim().to_string()),
        Value::Number(n) => Ok(n.to_string()),
        other => Err(format!("expected a NUMERIC literal, got `{other}`")),
    }
}

/// Parse + validate a decimal-text NUMERIC literal against an optional declared
/// `(precision, scale)` (CONCEPT:EG-KG.query.table-schema-constraints/NE-002). Rejects (never silently
/// truncates): a non-decimal literal, more fractional digits than `scale`, or more
/// total significant digits than `precision`. Returns the canonical form, its
/// fractional part zero-padded to exactly `scale` digits when a scale is declared.
fn normalize_numeric_literal(raw: &str, precision_scale: Option<(u32, u32)>) -> Result<String, String> {
    let s = raw.trim();
    let (sign, digits) = match s.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = match digits.split_once('.') {
        Some((i, f)) => (i, f),
        None => (digits, ""),
    };
    let int_part = if int_part.is_empty() { "0" } else { int_part };
    if int_part.is_empty()
        || !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(format!("invalid NUMERIC literal `{raw}`"));
    }
    let int_trimmed = int_part.trim_start_matches('0');
    let int_digits = if int_trimmed.is_empty() { 1 } else { int_trimmed.len() };
    let Some((precision, scale)) = precision_scale else {
        let frac = if frac_part.is_empty() {
            String::new()
        } else {
            format!(".{frac_part}")
        };
        let int_out = if int_trimmed.is_empty() { "0" } else { int_trimmed };
        return Ok(format!("{sign}{int_out}{frac}"));
    };
    let scale = scale as usize;
    if frac_part.len() > scale {
        return Err(format!(
            "NUMERIC literal `{raw}` has more than {scale} decimal digit(s)"
        ));
    }
    let max_int_digits = (precision as usize).saturating_sub(scale).max(1);
    if int_digits > max_int_digits {
        return Err(format!(
            "NUMERIC literal `{raw}` overflows precision {precision}, scale {scale}"
        ));
    }
    let mut padded_frac = frac_part.to_string();
    while padded_frac.len() < scale {
        padded_frac.push('0');
    }
    let int_out = if int_trimmed.is_empty() { "0" } else { int_trimmed };
    if scale == 0 {
        Ok(format!("{sign}{int_out}"))
    } else {
        Ok(format!("{sign}{int_out}.{padded_frac}"))
    }
}

/// Days since the Unix epoch (1970-01-01) for a proleptic-Gregorian civil date
/// (Howard Hinnant's `days_from_civil` — well-known constant-time formula, no
/// external date/time crate needed). Used only by [`parse_timestamptz`].
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = (m as i64 + 9) % 12; // [0, 11]
    let doy = (153 * mp + 2) / 5 + d as i64 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146097 + doe - 719468
}

/// Parse a TIMESTAMPTZ literal (CONCEPT:EG-KG.query.table-schema-constraints/NE-002): `YYYY-MM-DD[ T]HH:MM:SS[.ffffff]`
/// followed by an EXPLICIT zone — `Z`/`z`, or a signed `HH[:MM]`/`HHMM` offset.
/// Normalizes to UTC epoch microseconds. A zone-less literal is a hard error (the
/// whole point of the type: no silent local-time assumption).
fn parse_timestamptz(s: &str) -> Result<i64, String> {
    let raw = s.trim();
    let bad = || format!("invalid timestamptz literal `{s}` (an explicit UTC offset or `Z` is required)");
    let (date, rest) = raw.split_once(['T', 't', ' ']).ok_or_else(bad)?;
    let mut dparts = date.splitn(3, '-');
    let year: i64 = dparts.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
    let month: u32 = dparts.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
    let day: u32 = dparts.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(bad());
    }

    // Split the time-plus-offset tail. `Z`/`z` is a zero offset; otherwise find the
    // LAST `+`/`-` (the time-of-day itself never contains one).
    let (time_part, offset_minutes): (&str, i64) = if let Some(t) = rest
        .strip_suffix('Z')
        .or_else(|| rest.strip_suffix('z'))
    {
        (t, 0)
    } else if let Some(pos) = rest.rfind(['+', '-']) {
        let (t, off) = rest.split_at(pos);
        let sign = if off.starts_with('-') { -1i64 } else { 1i64 };
        let off = &off[1..];
        let (oh, om): (&str, &str) = if let Some((h, m)) = off.split_once(':') {
            (h, m)
        } else if off.len() >= 3 {
            off.split_at(off.len() - 2)
        } else {
            (off, "0")
        };
        let oh: i64 = oh.parse().map_err(|_| bad())?;
        let om: i64 = om.parse().map_err(|_| bad())?;
        (t, sign * (oh * 60 + om))
    } else {
        return Err(bad());
    };

    let mut tparts = time_part.splitn(3, ':');
    let hh: i64 = tparts.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
    let mm: i64 = tparts.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
    let sec_str = tparts.next().ok_or_else(bad)?;
    let (ss, frac_micros): (i64, i64) = if let Some((s, f)) = sec_str.split_once('.') {
        let ss: i64 = s.parse().map_err(|_| bad())?;
        let mut f = f.to_string();
        while f.len() < 6 {
            f.push('0');
        }
        f.truncate(6);
        let fm: i64 = f.parse().map_err(|_| bad())?;
        (ss, fm)
    } else {
        (sec_str.parse().map_err(|_| bad())?, 0)
    };
    if !(0..24).contains(&hh) || !(0..60).contains(&mm) || !(0..61).contains(&ss) {
        return Err(bad());
    }

    let days = days_from_civil(year, month, day);
    let micros = days * 86_400_000_000i64
        + (hh * 3600 + mm * 60 + ss) * 1_000_000i64
        + frac_micros
        - offset_minutes * 60_000_000i64;
    Ok(micros)
}

/// Parse a Postgres array TEXT literal `{a,b,c}` into a JSON array of strings (CONCEPT:EG-KG.query.table-schema-constraints/NE-002).
/// A quoted element (`"a b"`) has its surrounding quotes stripped; an unquoted `NULL`
/// (case-insensitive) decodes to JSON null. This is the TEXT-protocol array spelling;
/// the JSON-array spelling (`["a","b"]`) is handled directly by the `Value::Array` arm
/// in [`Cell::coerce`] and never reaches this function.
fn parse_pg_array_text(s: &str) -> Result<Vec<Value>, String> {
    let inner = s.trim();
    let inner = inner
        .strip_prefix('{')
        .and_then(|r| r.strip_suffix('}'))
        .ok_or_else(|| format!("invalid array literal `{s}` (expected `{{a,b,c}}`)"))?;
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }
    inner
        .split(',')
        .map(|tok| {
            let tok = tok.trim();
            if tok.eq_ignore_ascii_case("null") {
                Ok(Value::Null)
            } else if let Some(unquoted) = tok.strip_prefix('"').and_then(|r| r.strip_suffix('"')) {
                Ok(Value::String(unquoted.to_string()))
            } else {
                Ok(Value::String(tok.to_string()))
            }
        })
        .collect()
}

// ── SQL stored functions (CONCEPT:EG-KG.query.create-drop-function) ─────────────────────────────────────

/// One argument of a SQL stored function (CONCEPT:EG-KG.query.create-drop-function): `argname argtype`. The
/// `type_name` is the raw SQL type spelling (`int`, `text`, `double precision`, …);
/// execution relies on DataFusion's schema inference over the EXPANDED body, so the
/// declared type is catalog metadata (surfaced by a future `pg_proc`), not a coercion
/// applied here. Reused for a `RETURNS TABLE(col type, …)` column too.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionArg {
    pub name: String,
    pub type_name: String,
}

/// What a SQL stored function returns (CONCEPT:EG-KG.query.create-drop-function). A `Scalar` function's body is a
/// single-expression `SELECT` inlined into an expression; a `Table`/`SetOf` function's
/// body is a `SELECT` expanded as a parameterized-view subquery in a `FROM` clause.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FunctionReturns {
    /// `RETURNS <scalar type>` — the body is `SELECT <expr>`; a call inlines `<expr>`.
    Scalar(String),
    /// `RETURNS TABLE(col type, …)` — the declared output columns; the body is a SELECT.
    Table(Vec<FunctionArg>),
    /// `RETURNS SETOF <type>` — set-returning; the body SELECT drives the output schema.
    SetOf(String),
}

/// The procedural language a stored function's body is written in (CONCEPT:EG-KG.query.eg-validate-procedural-body).
/// `Sql` bodies are EXPANDED inline at plan time (CONCEPT:EG-KG.query.create-drop-function); `PlPgSql` bodies are
/// run through the procedural interpreter (`sql::plpgsql`) on a bare top-level call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FunctionLanguage {
    /// `LANGUAGE sql` — body is a single `SELECT`, expanded inline (CONCEPT:EG-KG.query.create-drop-function).
    Sql,
    /// `LANGUAGE plpgsql` — a procedural body (DECLARE/BEGIN..END, IF, loops, `:=`,
    /// `SELECT … INTO`, RETURN) executed by the interpreter (CONCEPT:EG-KG.query.eg-validate-procedural-body/EG-341).
    PlPgSql,
}

/// A durable stored function (CONCEPT:EG-KG.query.create-drop-function / EG-340), persisted in the function
/// catalog beside the view/table catalogs. A `LANGUAGE sql` body is EXPANDED into a
/// query at plan time so DataFusion's existing planner executes it (no separate
/// evaluator). A `LANGUAGE plpgsql` body (IF/LOOP/variables/RETURN) is executed by the
/// procedural interpreter (`sql::plpgsql`, CONCEPT:EG-KG.query.eg-validate-procedural-body/EG-341) when a bare top-level
/// `SELECT fn(args)` / `CALL fn(args)` names it — its embedded SQL runs back through the
/// same read path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredFunction {
    pub name: String,
    pub args: Vec<FunctionArg>,
    pub returns: FunctionReturns,
    /// The function body. For `Sql`: a dollar-/single-quoted `SELECT …` whose argument
    /// identifiers reference `args` by name. For `PlPgSql`: the procedural block.
    pub body: String,
    /// The body's procedural language (CONCEPT:EG-KG.query.eg-validate-procedural-body).
    pub language: FunctionLanguage,
}

impl StoredFunction {
    /// Whether this function returns a set of rows (`RETURNS TABLE(...)`/`SETOF`) — so a
    /// call is expanded as a `FROM`-clause subquery — versus a scalar function whose
    /// body expression is inlined into an expression (CONCEPT:EG-KG.query.create-drop-function).
    pub fn is_table(&self) -> bool {
        matches!(
            self.returns,
            FunctionReturns::Table(_) | FunctionReturns::SetOf(_)
        )
    }

    /// Whether this is a `LANGUAGE plpgsql` procedural function (CONCEPT:EG-KG.query.eg-validate-procedural-body) — run by
    /// the interpreter on a bare call rather than expanded inline like a `LANGUAGE sql`
    /// body. Such a function is EXCLUDED from `funcs::expand_functions` (its body is not
    /// SQL) so an embedded `fn(x)` in a larger query is left for the planner to reject.
    pub fn is_plpgsql(&self) -> bool {
        matches!(self.language, FunctionLanguage::PlPgSql)
    }
}
