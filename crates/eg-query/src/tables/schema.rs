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
use serde_json::Value;

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
}

impl ColumnType {
    /// Parse a SQL type name (case-insensitive, e.g. from `CREATE TABLE`) into a
    /// [`ColumnType`]. Accepts the common Postgres/standard spellings plus the
    /// engine's canonical names so `int4`/`integer`/`int` all land on `Int`, etc.
    /// Length/precision suffixes (`varchar(255)`, `numeric(10,2)`) are ignored — the
    /// base type is what the store needs. Returns `Err` for an unknown type.
    pub fn parse(name: &str) -> Result<ColumnType, String> {
        // Strip any `(...)` precision/length suffix and whitespace, lowercase.
        let base = name
            .split('(')
            .next()
            .unwrap_or(name)
            .trim()
            .to_ascii_lowercase();
        // CONCEPT:EG-KG.query.vector-json-array-render — pgvector `vector`/`vector(n)`. The dimension is read from the
        // ORIGINAL spelling (the `(n)` suffix that the base-type strip above discards).
        if base == "vector" {
            return Ok(ColumnType::Vector(parse_vector_dim(name)));
        }
        let ty = match base.as_str() {
            "int" | "int4" | "integer" | "serial" | "smallint" | "int2" => ColumnType::Int,
            "bigint" | "int8" | "bigserial" | "long" => ColumnType::BigInt,
            "float" | "float4" | "real" => ColumnType::Float,
            "double" | "float8" | "double precision" => ColumnType::Double,
            "text" | "varchar" | "char" | "character" | "character varying" | "string" | "uuid" => {
                ColumnType::Text
            }
            "bool" | "boolean" => ColumnType::Bool,
            "timestamp"
            | "timestamptz"
            | "datetime"
            | "timestamp with time zone"
            | "timestamp without time zone" => ColumnType::Timestamp,
            "bytes" | "bytea" | "blob" | "binary" => ColumnType::Bytes,
            "json" | "jsonb" => ColumnType::Json,
            other => return Err(format!("unsupported column type `{other}`")),
        };
        Ok(ty)
    }
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
        Self::new(self.name.clone(), self.columns.clone())
    }
}

impl PartialEq for TableSchema {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.columns == other.columns
    }
}

impl TableSchema {
    pub fn new(name: impl Into<String>, columns: Vec<Column>) -> Self {
        Self {
            name: name.into(),
            columns,
            column_offsets: OnceLock::new(),
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
            .map_err(|error| error.to_string())
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
