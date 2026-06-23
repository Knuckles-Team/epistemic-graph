//! Synthetic read-only Postgres catalog (CONCEPT:KG-2.201). Real Postgres
//! drivers/ORMs introspect the catalog on connect — psycopg/SQLAlchemy/JDBC/sqlx
//! issue `SELECT ... FROM information_schema.tables/columns`, probe
//! `pg_catalog.pg_class`/`pg_namespace`/`pg_type`/`pg_attribute`, and call
//! `version()` / `current_schema()` / `current_database()`. The engine only
//! exposes `nodes`/`edges`, so those introspection queries used to fail. This
//! module makes them succeed against a SYNTHETIC catalog derived from the live
//! graph schema, so a real client can connect → reflect/inspect → SELECT.
//!
//! ## Approach
//! Two complementary layers, registered into the SAME `SessionContext` the
//! `nodes`/`edges` query path builds (so a catalog query takes the identical
//! DataFusion execution path as any other read — no second SQL engine):
//!
//!   1. **`information_schema`** is DataFusion-native: enabling it on the
//!      `SessionConfig` auto-exposes `information_schema.tables`/`.columns`/… over
//!      whatever tables are registered (`nodes`, `edges`, and — schema-on-read —
//!      the inferred node property columns, which are real columns of the `nodes`
//!      provider's Arrow schema). So `information_schema` reflection is free and
//!      always in sync with the schema inference.
//!   2. **`pg_catalog`** is supplemented here as synthetic `MemTable`s
//!      (`pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`) built from the same
//!      inferred `nodes`/`edges` schemas, plus the common catalog scalar functions
//!      (`version()`, `current_schema()`, `current_database()`, `current_user`,
//!      `pg_catalog.pg_get_userbyid()` / `pg_get_expr()` no-ops) as zero-/fixed-arg
//!      UDFs. These are exactly what a `psycopg.connect()` + SQLAlchemy
//!      `inspect()`/`reflect()` probe.
//!
//! Everything here is READ-ONLY: the tables are in-memory snapshots of the schema
//! and the functions are pure. The catalog never changes a real query's result.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::SchemaProvider;
use datafusion::catalog_common::memory::MemorySchemaProvider;
use datafusion::datasource::MemTable;
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{
    create_udf, ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// The OID we synthesize for the single user schema (`public`). Drivers only use
/// OIDs to JOIN catalog rows together, so a stable arbitrary value is sufficient.
const PUBLIC_NAMESPACE_OID: i32 = 2200;
/// Base OID for synthetic relations; each registered table gets `BASE + index`.
const REL_OID_BASE: i32 = 16384;
/// Postgres type OIDs we map our coarse Arrow→pg types onto (real pg values, so a
/// driver that looks a column's type up in `pg_type` resolves a sane type name).
const OID_BOOL: i32 = 16;
const OID_INT8: i32 = 20;
const OID_FLOAT8: i32 = 701;
const OID_TEXT: i32 = 25;

/// One queryable relation the catalog describes: its name plus its `(column,
/// pg_type_oid)` list. Built from the live `nodes`/`edges` Arrow schemas so the
/// catalog always matches what a SELECT actually returns.
struct CatalogRelation {
    name: String,
    columns: Vec<(String, i32)>,
}

/// Map an Arrow `DataType` to the Postgres type OID the catalog reports for it,
/// matching the coarse wire mapping the pgwire shim uses (`PgColType`).
fn arrow_to_pg_oid(dt: &DataType) -> i32 {
    use DataType::*;
    match dt {
        Boolean => OID_BOOL,
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => OID_INT8,
        Float16 | Float32 | Float64 => OID_FLOAT8,
        _ => OID_TEXT,
    }
}

/// The pg `typname` for one of our reported type OIDs.
fn pg_typname(oid: i32) -> &'static str {
    match oid {
        OID_BOOL => "bool",
        OID_INT8 => "int8",
        OID_FLOAT8 => "float8",
        _ => "text",
    }
}

/// Build the [`CatalogRelation`] list from the inferred `nodes`/`edges` schemas.
/// The `props: Binary` escape-hatch column is reported as `text` (its values are
/// not introspectable structurally, and a driver never reflects against it).
fn relations(nodes_schema: &SchemaRef, edges_schema: &SchemaRef) -> Vec<CatalogRelation> {
    let to_cols = |schema: &SchemaRef| -> Vec<(String, i32)> {
        schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), arrow_to_pg_oid(f.data_type())))
            .collect()
    };
    vec![
        CatalogRelation {
            name: "nodes".to_string(),
            columns: to_cols(nodes_schema),
        },
        CatalogRelation {
            name: "edges".to_string(),
            columns: to_cols(edges_schema),
        },
    ]
}

/// `pg_namespace`: one row for the single `public` schema (plus `pg_catalog` and
/// `information_schema` so a `SELECT ... WHERE nspname = 'public'` join resolves).
fn pg_namespace_batch() -> Result<(SchemaRef, RecordBatch), String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", DataType::Int32, false),
        Field::new("nspname", DataType::Utf8, false),
        Field::new("nspowner", DataType::Int32, false),
    ]));
    let oids = Int32Array::from(vec![PUBLIC_NAMESPACE_OID, 11, 12]);
    let names = StringArray::from(vec!["public", "pg_catalog", "information_schema"]);
    let owners = Int32Array::from(vec![10, 10, 10]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(oids), Arc::new(names), Arc::new(owners)],
    )
    .map_err(|e| format!("pg_namespace batch: {e}"))?;
    Ok((schema, batch))
}

/// `pg_class`: one row per queryable relation. `relkind='r'` (ordinary table),
/// `relnamespace` points at the `public` schema oid.
fn pg_class_batch(rels: &[CatalogRelation]) -> Result<(SchemaRef, RecordBatch), String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", DataType::Int32, false),
        Field::new("relname", DataType::Utf8, false),
        Field::new("relnamespace", DataType::Int32, false),
        Field::new("relkind", DataType::Utf8, false),
        Field::new("relam", DataType::Int32, false),
        Field::new("reltuples", DataType::Float64, false),
        Field::new("relhasindex", DataType::Boolean, false),
        Field::new("relpersistence", DataType::Utf8, false),
    ]));
    let mut oids = Vec::new();
    let mut names = Vec::new();
    for (i, r) in rels.iter().enumerate() {
        oids.push(REL_OID_BASE + i as i32);
        names.push(r.name.clone());
    }
    let n = rels.len();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(oids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(vec![PUBLIC_NAMESPACE_OID; n])),
            Arc::new(StringArray::from(vec!["r"; n])),
            Arc::new(Int32Array::from(vec![0; n])),
            Arc::new(arrow::array::Float64Array::from(vec![0.0; n])),
            Arc::new(BooleanArray::from(vec![false; n])),
            Arc::new(StringArray::from(vec!["p"; n])),
        ],
    )
    .map_err(|e| format!("pg_class batch: {e}"))?;
    Ok((schema, batch))
}

/// `pg_attribute`: one row per (relation, column). `attnum` is 1-based; the
/// `props` escape-hatch column is included for completeness. `atttypid` is the
/// reported pg type OID so a reflecting driver resolves each column's type.
fn pg_attribute_batch(rels: &[CatalogRelation]) -> Result<(SchemaRef, RecordBatch), String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("attrelid", DataType::Int32, false),
        Field::new("attname", DataType::Utf8, false),
        Field::new("atttypid", DataType::Int32, false),
        Field::new("attnum", DataType::Int32, false),
        Field::new("attnotnull", DataType::Boolean, false),
        Field::new("atttypmod", DataType::Int32, false),
        Field::new("attisdropped", DataType::Boolean, false),
    ]));
    let mut relids = Vec::new();
    let mut names = Vec::new();
    let mut typids = Vec::new();
    let mut nums = Vec::new();
    for (i, r) in rels.iter().enumerate() {
        let relid = REL_OID_BASE + i as i32;
        for (col_idx, (col, oid)) in r.columns.iter().enumerate() {
            relids.push(relid);
            names.push(col.clone());
            typids.push(*oid);
            nums.push(col_idx as i32 + 1);
        }
    }
    let total = names.len();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(relids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(typids)),
            Arc::new(Int32Array::from(nums)),
            Arc::new(BooleanArray::from(vec![false; total])),
            Arc::new(Int32Array::from(vec![-1; total])),
            Arc::new(BooleanArray::from(vec![false; total])),
        ],
    )
    .map_err(|e| format!("pg_attribute batch: {e}"))?;
    Ok((schema, batch))
}

/// `pg_type`: the small fixed set of types the catalog reports (bool/int8/float8/
/// text). A reflecting driver looks up a column's `atttypid` here to name its type.
fn pg_type_batch() -> Result<(SchemaRef, RecordBatch), String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", DataType::Int32, false),
        Field::new("typname", DataType::Utf8, false),
        Field::new("typnamespace", DataType::Int32, false),
        Field::new("typtype", DataType::Utf8, false),
        Field::new("typelem", DataType::Int32, false),
    ]));
    let oids = [OID_BOOL, OID_INT8, OID_FLOAT8, OID_TEXT];
    let names: Vec<&str> = oids.iter().map(|o| pg_typname(*o)).collect();
    let n = oids.len();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(oids.to_vec())),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(vec![11; n])), // pg_catalog namespace
            Arc::new(StringArray::from(vec!["b"; n])), // base type
            Arc::new(Int32Array::from(vec![0; n])),
        ],
    )
    .map_err(|e| format!("pg_type batch: {e}"))?;
    Ok((schema, batch))
}

/// A zero-argument constant-string scalar function (`version()` /
/// `current_schema()` / `current_database()` / `current_user`). Implemented as a
/// `ScalarUDFImpl` so DataFusion's `invoke_no_args` path (used for a zero-arity
/// call like `SELECT version()`) returns the fixed value — `create_udf` with an
/// empty signature does NOT implement `invoke_no_args`.
#[derive(Debug)]
struct ConstStringUdf {
    name: String,
    value: String,
    signature: Signature,
}

impl ConstStringUdf {
    fn new(name: &str, value: &str) -> Self {
        Self {
            name: name.to_string(),
            value: value.to_string(),
            // Zero-argument, stable (constant within a query).
            signature: Signature::exact(vec![], Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for ConstStringUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }
    /// Zero-arg constant: a zero-arity call routes here with an empty `args` slice;
    /// return the fixed value broadcast over `number_rows` (a scalar `SELECT
    /// version()` is one row). Implements the non-deprecated batch entry point.
    fn invoke_batch(&self, _args: &[ColumnarValue], number_rows: usize) -> DfResult<ColumnarValue> {
        let rows = number_rows.max(1);
        let arr: ArrayRef = Arc::new(StringArray::from(vec![self.value.clone(); rows]));
        Ok(ColumnarValue::Array(arr))
    }
}

/// Build the constant-string catalog function as a registrable [`ScalarUDF`].
fn const_string_udf(name: &str, value: &str) -> ScalarUDF {
    ScalarUDF::from(ConstStringUdf::new(name, value))
}

/// A single-argument no-op that returns its input rendered as text (or NULL). Used
/// for `pg_get_expr` / `pg_get_userbyid` / `format_type` style catalog helpers a
/// driver may call while reflecting; we never have real defaults/owners, so a TEXT
/// passthrough (or empty string) keeps the reflect query running.
fn passthrough_text_udf(name: &str) -> ScalarUDF {
    let fun = Arc::new(|args: &[ColumnarValue]| {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        let n = arrays.first().map(|a| a.len()).unwrap_or(1);
        // Always return an empty string — these helpers describe defaults/ACLs we
        // do not model; an empty rendering is the safe, non-failing answer.
        let arr: ArrayRef = Arc::new(StringArray::from(vec![Some(""); n]));
        Ok(ColumnarValue::Array(arr))
    });
    // Accept any single arg type by declaring it over Utf8; DataFusion coerces.
    create_udf(
        name,
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Stable,
        fun,
    )
}

/// Register a `MemTable` under the `pg_catalog` schema, creating the schema on the
/// default catalog the first time. Returns a clean error string on failure.
fn register_pg_table(
    ctx: &SessionContext,
    pg_schema: &Arc<MemorySchemaProvider>,
    name: &str,
    table: (SchemaRef, RecordBatch),
) -> Result<(), String> {
    let mem = MemTable::try_new(table.0, vec![vec![table.1]])
        .map_err(|e| format!("pg_catalog.{name} memtable: {e}"))?;
    let _ = ctx;
    pg_schema
        .register_table(name.to_string(), Arc::new(mem))
        .map_err(|e| format!("register pg_catalog.{name}: {e}"))?;
    Ok(())
}

/// Register the synthetic `pg_catalog` schema (tables + scalar functions) into
/// `ctx` (CONCEPT:KG-2.201), derived from the inferred `nodes`/`edges` schemas.
/// `information_schema` is enabled on the `SessionContext`'s config separately (in
/// the exec builder) — this only supplies the `pg_catalog` supplement DataFusion
/// does not provide. Idempotent per context (called once per built context).
pub(crate) fn register_pg_catalog(
    ctx: &SessionContext,
    nodes_schema: &SchemaRef,
    edges_schema: &SchemaRef,
) -> Result<(), String> {
    let rels = relations(nodes_schema, edges_schema);

    // Attach a `pg_catalog` schema to the default catalog and populate it.
    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(|| "default catalog 'datafusion' missing".to_string())?;
    let pg_schema = Arc::new(MemorySchemaProvider::new());
    register_pg_table(ctx, &pg_schema, "pg_namespace", pg_namespace_batch()?)?;
    register_pg_table(ctx, &pg_schema, "pg_class", pg_class_batch(&rels)?)?;
    register_pg_table(ctx, &pg_schema, "pg_attribute", pg_attribute_batch(&rels)?)?;
    register_pg_table(ctx, &pg_schema, "pg_type", pg_type_batch()?)?;
    catalog
        .register_schema("pg_catalog", pg_schema)
        .map_err(|e| format!("register pg_catalog schema: {e}"))?;

    // Catalog scalar functions drivers probe on connect.
    ctx.register_udf(const_string_udf(
        "version",
        "PostgreSQL 16.6 (epistemic-graph pgwire) on x86_64-epistemic, compiled by rustc",
    ));
    ctx.register_udf(const_string_udf("current_schema", "public"));
    ctx.register_udf(const_string_udf("current_database", "epistemic-graph"));
    ctx.register_udf(const_string_udf("current_user", "epistemic"));
    ctx.register_udf(const_string_udf("session_user", "epistemic"));
    ctx.register_udf(passthrough_text_udf("pg_get_userbyid"));
    ctx.register_udf(passthrough_text_udf("pg_get_expr"));
    ctx.register_udf(passthrough_text_udf("pg_get_constraintdef"));
    ctx.register_udf(passthrough_text_udf("format_type"));

    Ok(())
}
