//! Synthetic read-only Postgres system catalogs — `pg_catalog` + `information_schema`
//! (CONCEPT:EG-103, extending the original `pg_catalog` supplement CONCEPT:KG-2.201).
//!
//! Real Postgres clients introspect the system catalogs on connect and while
//! reflecting: `psql`'s `\d`/`\dt`, psycopg/SQLAlchemy/JDBC/sqlx `SELECT ... FROM
//! information_schema.tables/columns`, and ORM bootstrap queries over
//! `pg_catalog.pg_class JOIN pg_catalog.pg_namespace`, `pg_attribute`, `pg_type`,
//! `pg_proc`, `pg_index`, calling `pg_catalog.pg_table_is_visible(oid)`,
//! `format_type(oid, typmod)`, `current_schema()`, `current_database()`, `version()`,
//! `obj_description(oid, catalog)`. The engine only physically stores `nodes`/`edges`
//! + user tables + views + SQL functions, so this module makes those introspection
//! queries succeed against SYNTHETIC catalogs derived FROM those live catalogs, so a
//! real client can connect → reflect/inspect → SELECT (a genuine drop-in).
//!
//! ## Approach (CONCEPT:EG-103)
//! Everything is synthesized as read-only `MemTable`s registered into the SAME
//! `SessionContext` the `nodes`/`edges` query path builds, so a catalog query takes
//! the identical DataFusion execution path as any other read — there is no second SQL
//! engine, and every row is recomputed per query from the live relations.
//!
//!   * **`pg_catalog`** — `pg_class` (relations: `nodes`/`edges`/user tables →
//!     `relkind='r'`, views → `relkind='v'`), `pg_namespace` (schemas), `pg_attribute`
//!     (columns), `pg_type` (the pg type OIDs the pgwire shim already uses),
//!     `pg_index` (shaped, empty), `pg_proc` (the EG-118 SQL functions), `pg_database`,
//!     `pg_settings` (minimal).
//!   * **`information_schema`** — `tables`, `columns`, `schemata`, `views`, `routines`,
//!     `key_column_usage` (minimal), `table_constraints` (minimal). DataFusion's NATIVE
//!     `information_schema` is disabled in the exec builder because it cannot be extended
//!     with `routines`/`key_column_usage`/`table_constraints`; synthesizing the whole
//!     schema keeps it consistent with `pg_catalog` and in sync with the schema-on-read
//!     inference (`tables`/`columns` are rebuilt from the same live relations).
//!   * **catalog functions** — `version()`/`current_schema()`/`current_database()`/
//!     `current_user`/`session_user`, `pg_catalog.pg_table_is_visible(oid)`,
//!     `format_type(oid, typmod)`, `obj_description(...)`, and the `pg_get_*` reflect
//!     helpers, registered as scalar UDFs (NULL / sensible defaults where full fidelity
//!     isn't feasible).
//!
//! Everything here is READ-ONLY and never changes a real query's result.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::SchemaProvider;
use datafusion::catalog_common::memory::MemorySchemaProvider;
use datafusion::datasource::MemTable;
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;

use crate::tables::StoredFunction;

/// The OID we synthesize for the single user schema (`public`). Drivers only use OIDs
/// to JOIN catalog rows together, so a stable arbitrary value is sufficient.
const PUBLIC_NAMESPACE_OID: i32 = 2200;
/// `pg_catalog` schema OID (matches Postgres' fixed value).
const PG_CATALOG_NAMESPACE_OID: i32 = 11;
/// `information_schema` schema OID (arbitrary-but-stable, distinct from the others).
const INFO_SCHEMA_NAMESPACE_OID: i32 = 12;
/// Base OID for synthetic relations; each relation gets `BASE + index`.
const REL_OID_BASE: i32 = 16384;
/// Base OID for synthetic procedures (`pg_proc`); each function gets `BASE + index`.
const PROC_OID_BASE: i32 = 24576;
/// The single synthetic database's OID (`pg_database`).
const DATABASE_OID: i32 = 16400;

/// Postgres type OIDs (real pg values, so a driver that looks a column's type up in
/// `pg_type` resolves a sane type name). The four the engine actually emits reuse the
/// SAME OIDs the pgwire shim maps `PgColType` onto (`bool`/`int8`/`float8`/`text`).
const OID_BOOL: i32 = 16;
const OID_INT8: i32 = 20;
const OID_FLOAT8: i32 = 701;
const OID_TEXT: i32 = 25;

/// Postgres `relkind` of a synthesized relation (CONCEPT:EG-103).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelKind {
    /// Ordinary table (`nodes`, `edges`, a user `CREATE TABLE`) — `relkind='r'`.
    Table,
    /// A durable view (EG-072 `CREATE VIEW`) — `relkind='v'`.
    View,
}

impl RelKind {
    /// The single-char `pg_class.relkind` value.
    fn relkind(self) -> &'static str {
        match self {
            RelKind::Table => "r",
            RelKind::View => "v",
        }
    }
    /// The `information_schema.tables.table_type` value.
    fn table_type(self) -> &'static str {
        match self {
            RelKind::Table => "BASE TABLE",
            RelKind::View => "VIEW",
        }
    }
}

/// One column of a synthesized relation: its name, reported pg type OID, and 1-based
/// ordinal position.
struct CatalogColumn {
    name: String,
    type_oid: i32,
    ordinal: i32,
}

/// One queryable relation the catalog describes (CONCEPT:EG-103): its name, assigned
/// OID, kind (table/view), and columns. Built from the live `nodes`/`edges`/user-table
/// Arrow schemas + the registered view providers, so the catalog always matches what a
/// SELECT actually returns.
struct CatalogRelation {
    name: String,
    oid: i32,
    kind: RelKind,
    columns: Vec<CatalogColumn>,
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

/// The pg `typname` for a reported type OID (extended set so a driver that looks up any
/// common OID still resolves a name; the engine itself only emits the four coarse ones).
fn pg_typname(oid: i32) -> &'static str {
    match oid {
        OID_BOOL => "bool",
        21 => "int2",
        23 => "int4",
        OID_INT8 => "int8",
        26 => "oid",
        700 => "float4",
        OID_FLOAT8 => "float8",
        1043 => "varchar",
        1114 => "timestamp",
        17 => "bytea",
        19 => "name",
        1021 => "_float4",
        _ => "text",
    }
}

/// The `information_schema.columns.data_type` (SQL standard spelling) + `udt_name` for a
/// reported type OID. Where the engine's coarse mapping cannot distinguish (everything
/// non-numeric/bool collapses to `text`), a plausible standard name is returned.
fn oid_to_sql_type(oid: i32) -> (&'static str, &'static str) {
    match oid {
        OID_BOOL => ("boolean", "bool"),
        OID_INT8 => ("bigint", "int8"),
        OID_FLOAT8 => ("double precision", "float8"),
        _ => ("text", "text"),
    }
}

/// Map a declared SQL type spelling (from an EG-118 function's arg/return type) to a
/// reported pg type OID, for `pg_proc.prorettype`. Coarse; defaults to `text`.
fn sql_type_name_to_oid(name: &str) -> i32 {
    let base = name
        .split('(')
        .next()
        .unwrap_or(name)
        .trim()
        .to_ascii_lowercase();
    match base.as_str() {
        "bool" | "boolean" => OID_BOOL,
        "int" | "int4" | "integer" | "smallint" | "int2" | "bigint" | "int8" | "long" => OID_INT8,
        "float" | "float4" | "real" | "double" | "float8" | "double precision" => OID_FLOAT8,
        _ => OID_TEXT,
    }
}

/// Collect the live relations (CONCEPT:EG-103): `nodes`/`edges` + user tables as
/// `relkind='r'` tables, then each durable view as `relkind='v'`. A view's columns are
/// read back from its now-registered DataFusion provider (best-effort: a view that
/// failed to register is still listed, with no columns). OIDs are assigned densely from
/// [`REL_OID_BASE`] in a stable order so JOINs across the catalog resolve.
async fn collect_relations(
    ctx: &SessionContext,
    nodes_schema: &SchemaRef,
    edges_schema: &SchemaRef,
    user: &[(String, SchemaRef)],
    views: &[(String, String)],
) -> Vec<CatalogRelation> {
    let cols_of = |schema: &SchemaRef| -> Vec<CatalogColumn> {
        schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| CatalogColumn {
                name: f.name().clone(),
                type_oid: arrow_to_pg_oid(f.data_type()),
                ordinal: i as i32 + 1,
            })
            .collect()
    };

    let mut rels: Vec<CatalogRelation> = Vec::new();
    let mut next_oid = REL_OID_BASE;
    let mut push = |name: String, kind: RelKind, columns: Vec<CatalogColumn>, rels: &mut Vec<_>| {
        rels.push(CatalogRelation {
            name,
            oid: next_oid,
            kind,
            columns,
        });
        next_oid += 1;
    };

    push(
        "nodes".to_string(),
        RelKind::Table,
        cols_of(nodes_schema),
        &mut rels,
    );
    push(
        "edges".to_string(),
        RelKind::Table,
        cols_of(edges_schema),
        &mut rels,
    );
    for (name, schema) in user {
        push(name.clone(), RelKind::Table, cols_of(schema), &mut rels);
    }
    for (name, _select) in views {
        // Read the view's real output columns from its registered provider.
        let columns = match ctx.table(name.as_str()).await {
            Ok(df) => df
                .schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| CatalogColumn {
                    name: f.name().clone(),
                    type_oid: arrow_to_pg_oid(f.data_type()),
                    ordinal: i as i32 + 1,
                })
                .collect(),
            Err(_) => Vec::new(),
        };
        push(name.clone(), RelKind::View, columns, &mut rels);
    }
    rels
}

// ── pg_catalog tables ─────────────────────────────────────────────────────────

/// `pg_namespace`: one row per schema (`public`, `pg_catalog`, `information_schema`).
fn pg_namespace_batch() -> Result<(SchemaRef, RecordBatch), String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", DataType::Int32, false),
        Field::new("nspname", DataType::Utf8, false),
        Field::new("nspowner", DataType::Int32, false),
    ]));
    let oids = Int32Array::from(vec![
        PUBLIC_NAMESPACE_OID,
        PG_CATALOG_NAMESPACE_OID,
        INFO_SCHEMA_NAMESPACE_OID,
    ]);
    let names = StringArray::from(vec!["public", "pg_catalog", "information_schema"]);
    let owners = Int32Array::from(vec![10, 10, 10]);
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(oids), Arc::new(names), Arc::new(owners)],
    )
    .map(|b| (schema, b))
    .map_err(|e| format!("pg_namespace batch: {e}"))
}

/// `pg_class`: one row per queryable relation. `relkind` is `'r'` for a table and `'v'`
/// for a view; `relnamespace` points at the `public` schema OID.
fn pg_class_batch(rels: &[CatalogRelation]) -> Result<(SchemaRef, RecordBatch), String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", DataType::Int32, false),
        Field::new("relname", DataType::Utf8, false),
        Field::new("relnamespace", DataType::Int32, false),
        Field::new("relkind", DataType::Utf8, false),
        Field::new("relam", DataType::Int32, false),
        Field::new("reltuples", DataType::Float64, false),
        Field::new("relnatts", DataType::Int32, false),
        Field::new("relhasindex", DataType::Boolean, false),
        Field::new("relpersistence", DataType::Utf8, false),
        Field::new("relowner", DataType::Int32, false),
        Field::new("reltablespace", DataType::Int32, false),
    ]));
    let n = rels.len();
    let oids: Vec<i32> = rels.iter().map(|r| r.oid).collect();
    let names: Vec<String> = rels.iter().map(|r| r.name.clone()).collect();
    let kinds: Vec<&str> = rels.iter().map(|r| r.kind.relkind()).collect();
    let natts: Vec<i32> = rels.iter().map(|r| r.columns.len() as i32).collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(oids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(vec![PUBLIC_NAMESPACE_OID; n])),
            Arc::new(StringArray::from(kinds)),
            Arc::new(Int32Array::from(vec![0; n])),
            Arc::new(Float64Array::from(vec![0.0; n])),
            Arc::new(Int32Array::from(natts)),
            Arc::new(BooleanArray::from(vec![false; n])),
            Arc::new(StringArray::from(vec!["p"; n])),
            Arc::new(Int32Array::from(vec![10; n])),
            Arc::new(Int32Array::from(vec![0; n])),
        ],
    )
    .map(|b| (schema, b))
    .map_err(|e| format!("pg_class batch: {e}"))
}

/// `pg_attribute`: one row per (relation, column). `attnum` is 1-based; `atttypid` is
/// the reported pg type OID so a reflecting driver resolves each column's type.
fn pg_attribute_batch(rels: &[CatalogRelation]) -> Result<(SchemaRef, RecordBatch), String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("attrelid", DataType::Int32, false),
        Field::new("attname", DataType::Utf8, false),
        Field::new("atttypid", DataType::Int32, false),
        Field::new("attnum", DataType::Int32, false),
        Field::new("attnotnull", DataType::Boolean, false),
        Field::new("atttypmod", DataType::Int32, false),
        Field::new("attisdropped", DataType::Boolean, false),
        Field::new("attndims", DataType::Int32, false),
        Field::new("atthasdef", DataType::Boolean, false),
    ]));
    let mut relids = Vec::new();
    let mut names = Vec::new();
    let mut typids = Vec::new();
    let mut nums = Vec::new();
    for r in rels {
        for c in &r.columns {
            relids.push(r.oid);
            names.push(c.name.clone());
            typids.push(c.type_oid);
            nums.push(c.ordinal);
        }
    }
    let total = names.len();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(relids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(typids)),
            Arc::new(Int32Array::from(nums)),
            Arc::new(BooleanArray::from(vec![false; total])),
            Arc::new(Int32Array::from(vec![-1; total])),
            Arc::new(BooleanArray::from(vec![false; total])),
            Arc::new(Int32Array::from(vec![0; total])),
            Arc::new(BooleanArray::from(vec![false; total])),
        ],
    )
    .map(|b| (schema, b))
    .map_err(|e| format!("pg_attribute batch: {e}"))
}

/// `pg_type`: the common set of pg types (bool/int2/int4/int8/float4/float8/text/
/// varchar/timestamp/bytea/name/oid/_float4). A reflecting driver looks up a column's
/// `atttypid` here to name its type; the four the engine emits are always present.
fn pg_type_batch() -> Result<(SchemaRef, RecordBatch), String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", DataType::Int32, false),
        Field::new("typname", DataType::Utf8, false),
        Field::new("typnamespace", DataType::Int32, false),
        Field::new("typtype", DataType::Utf8, false),
        Field::new("typelem", DataType::Int32, false),
        Field::new("typlen", DataType::Int32, false),
    ]));
    let oids: Vec<i32> = vec![
        OID_BOOL, 21, 23, OID_INT8, 26, 700, OID_FLOAT8, OID_TEXT, 1043, 1114, 17, 19, 1021,
    ];
    let names: Vec<&str> = oids.iter().map(|o| pg_typname(*o)).collect();
    // typlen: fixed for scalar bool/ints/floats, -1 (varlena) for text/varchar/bytea/array.
    let lens: Vec<i32> = oids
        .iter()
        .map(|o| match *o {
            OID_BOOL => 1,
            21 => 2,
            23 | 26 | 700 => 4,
            OID_INT8 | OID_FLOAT8 | 1114 => 8,
            _ => -1,
        })
        .collect();
    let n = oids.len();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(oids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(vec![PG_CATALOG_NAMESPACE_OID; n])),
            Arc::new(StringArray::from(vec!["b"; n])), // base type
            Arc::new(Int32Array::from(vec![0; n])),
            Arc::new(Int32Array::from(lens)),
        ],
    )
    .map(|b| (schema, b))
    .map_err(|e| format!("pg_type batch: {e}"))
}

/// `pg_index`: shaped but empty (CONCEPT:EG-103). The engine's secondary indexes are
/// implicit (index-pushdown providers), not first-class `pg_index` rows; an empty table
/// is the faithful "no user-visible indexes" answer and keeps a `\d`-style join running.
fn pg_index_batch() -> Result<(SchemaRef, RecordBatch), String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("indexrelid", DataType::Int32, false),
        Field::new("indrelid", DataType::Int32, false),
        Field::new("indnatts", DataType::Int32, false),
        Field::new("indisunique", DataType::Boolean, false),
        Field::new("indisprimary", DataType::Boolean, false),
        Field::new("indisclustered", DataType::Boolean, false),
        Field::new("indkey", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Arc::new(BooleanArray::from(Vec::<bool>::new())),
            Arc::new(BooleanArray::from(Vec::<bool>::new())),
            Arc::new(BooleanArray::from(Vec::<bool>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ],
    )
    .map(|b| (schema, b))
    .map_err(|e| format!("pg_index batch: {e}"))
}

/// `pg_proc`: one row per durable SQL stored function (CONCEPT:EG-118). `prokind='f'`;
/// `prorettype` maps the declared scalar return type (a table/setof function reports
/// `record`-ish `text`); `prosrc` is the stored body.
fn pg_proc_batch(functions: &[StoredFunction]) -> Result<(SchemaRef, RecordBatch), String> {
    use crate::tables::FunctionReturns;
    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", DataType::Int32, false),
        Field::new("proname", DataType::Utf8, false),
        Field::new("pronamespace", DataType::Int32, false),
        Field::new("prorettype", DataType::Int32, false),
        Field::new("pronargs", DataType::Int32, false),
        Field::new("prokind", DataType::Utf8, false),
        Field::new("proretset", DataType::Boolean, false),
        Field::new("prosrc", DataType::Utf8, false),
    ]));
    let mut oids = Vec::new();
    let mut names = Vec::new();
    let mut rettypes = Vec::new();
    let mut nargs = Vec::new();
    let mut retset = Vec::new();
    let mut src = Vec::new();
    for (i, f) in functions.iter().enumerate() {
        oids.push(PROC_OID_BASE + i as i32);
        names.push(f.name.clone());
        let rettype = match &f.returns {
            FunctionReturns::Scalar(t) => sql_type_name_to_oid(t),
            // A set/table function's row type isn't a scalar pg type; report `text`.
            FunctionReturns::Table(_) | FunctionReturns::SetOf(_) => OID_TEXT,
        };
        rettypes.push(rettype);
        nargs.push(f.args.len() as i32);
        retset.push(f.is_table());
        src.push(f.body.clone());
    }
    let n = functions.len();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(oids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(vec![PUBLIC_NAMESPACE_OID; n])),
            Arc::new(Int32Array::from(rettypes)),
            Arc::new(Int32Array::from(nargs)),
            Arc::new(StringArray::from(vec!["f"; n])),
            Arc::new(BooleanArray::from(retset)),
            Arc::new(StringArray::from(src)),
        ],
    )
    .map(|b| (schema, b))
    .map_err(|e| format!("pg_proc batch: {e}"))
}

/// `pg_database`: the single synthetic database row (`epistemic-graph`).
fn pg_database_batch() -> Result<(SchemaRef, RecordBatch), String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", DataType::Int32, false),
        Field::new("datname", DataType::Utf8, false),
        Field::new("datdba", DataType::Int32, false),
        Field::new("encoding", DataType::Int32, false),
        Field::new("datcollate", DataType::Utf8, false),
        Field::new("datctype", DataType::Utf8, false),
        Field::new("datistemplate", DataType::Boolean, false),
        Field::new("datallowconn", DataType::Boolean, false),
    ]));
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![DATABASE_OID])),
            Arc::new(StringArray::from(vec!["epistemic-graph"])),
            Arc::new(Int32Array::from(vec![10])),
            Arc::new(Int32Array::from(vec![6])), // 6 = UTF8
            Arc::new(StringArray::from(vec!["en_US.UTF-8"])),
            Arc::new(StringArray::from(vec!["en_US.UTF-8"])),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(BooleanArray::from(vec![true])),
        ],
    )
    .map(|b| (schema, b))
    .map_err(|e| format!("pg_database batch: {e}"))
}

/// `pg_settings`: a minimal set of GUCs a client may probe (`name`/`setting` plus a few
/// descriptive columns). Not exhaustive — the values a `psql`/ORM connect commonly reads.
fn pg_settings_batch() -> Result<(SchemaRef, RecordBatch), String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("setting", DataType::Utf8, false),
        Field::new("unit", DataType::Utf8, true),
        Field::new("category", DataType::Utf8, false),
        Field::new("short_desc", DataType::Utf8, false),
        Field::new("vartype", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
    ]));
    let rows: &[(&str, &str, &str)] = &[
        ("server_version", "16.6", "string"),
        ("server_encoding", "UTF8", "string"),
        ("client_encoding", "UTF8", "string"),
        ("DateStyle", "ISO, MDY", "string"),
        ("TimeZone", "UTC", "string"),
        ("standard_conforming_strings", "on", "bool"),
        ("integer_datetimes", "on", "bool"),
        ("max_connections", "100", "integer"),
    ];
    let names: Vec<&str> = rows.iter().map(|r| r.0).collect();
    let settings: Vec<&str> = rows.iter().map(|r| r.1).collect();
    let vartypes: Vec<&str> = rows.iter().map(|r| r.2).collect();
    let n = rows.len();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(settings)),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            Arc::new(StringArray::from(vec!["epistemic-graph"; n])),
            Arc::new(StringArray::from(vec![""; n])),
            Arc::new(StringArray::from(vartypes)),
            Arc::new(StringArray::from(vec!["default"; n])),
        ],
    )
    .map(|b| (schema, b))
    .map_err(|e| format!("pg_settings batch: {e}"))
}

// ── information_schema tables ─────────────────────────────────────────────────

/// `information_schema.schemata`: one row per schema.
fn info_schemata_batch() -> Result<(SchemaRef, RecordBatch), String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("schema_owner", DataType::Utf8, false),
    ]));
    let n = 3;
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["epistemic-graph"; n])),
            Arc::new(StringArray::from(vec![
                "public",
                "pg_catalog",
                "information_schema",
            ])),
            Arc::new(StringArray::from(vec!["epistemic"; n])),
        ],
    )
    .map(|b| (schema, b))
    .map_err(|e| format!("information_schema.schemata batch: {e}"))
}

/// `information_schema.tables`: one row per relation, `table_type` = `BASE TABLE`/`VIEW`.
fn info_tables_batch(rels: &[CatalogRelation]) -> Result<(SchemaRef, RecordBatch), String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("table_catalog", DataType::Utf8, false),
        Field::new("table_schema", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_type", DataType::Utf8, false),
    ]));
    let n = rels.len();
    let names: Vec<String> = rels.iter().map(|r| r.name.clone()).collect();
    let types: Vec<&str> = rels.iter().map(|r| r.kind.table_type()).collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["epistemic-graph"; n])),
            Arc::new(StringArray::from(vec!["public"; n])),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(types)),
        ],
    )
    .map(|b| (schema, b))
    .map_err(|e| format!("information_schema.tables batch: {e}"))
}

/// `information_schema.columns`: one row per (relation, column), with the SQL-standard
/// `data_type`/`udt_name` and `ordinal_position` a reflecting ORM reads.
fn info_columns_batch(rels: &[CatalogRelation]) -> Result<(SchemaRef, RecordBatch), String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("table_catalog", DataType::Utf8, false),
        Field::new("table_schema", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("ordinal_position", DataType::Int32, false),
        Field::new("is_nullable", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("udt_name", DataType::Utf8, false),
    ]));
    let mut tnames = Vec::new();
    let mut cnames = Vec::new();
    let mut ords = Vec::new();
    let mut dtypes = Vec::new();
    let mut udts = Vec::new();
    for r in rels {
        for c in &r.columns {
            tnames.push(r.name.clone());
            cnames.push(c.name.clone());
            ords.push(c.ordinal);
            let (dt, udt) = oid_to_sql_type(c.type_oid);
            dtypes.push(dt);
            udts.push(udt);
        }
    }
    let n = cnames.len();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["epistemic-graph"; n])),
            Arc::new(StringArray::from(vec!["public"; n])),
            Arc::new(StringArray::from(tnames)),
            Arc::new(StringArray::from(cnames)),
            Arc::new(Int32Array::from(ords)),
            // Nullability isn't tracked structurally in the schema-on-read projection;
            // report `YES` (a plausible, non-failing default). CONCEPT:EG-103.
            Arc::new(StringArray::from(vec!["YES"; n])),
            Arc::new(StringArray::from(dtypes)),
            Arc::new(StringArray::from(udts)),
        ],
    )
    .map(|b| (schema, b))
    .map_err(|e| format!("information_schema.columns batch: {e}"))
}

/// `information_schema.views`: one row per durable view, with its stored SELECT text.
fn info_views_batch(views: &[(String, String)]) -> Result<(SchemaRef, RecordBatch), String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("table_catalog", DataType::Utf8, false),
        Field::new("table_schema", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("view_definition", DataType::Utf8, false),
        Field::new("check_option", DataType::Utf8, false),
        Field::new("is_updatable", DataType::Utf8, false),
    ]));
    let n = views.len();
    let names: Vec<String> = views.iter().map(|(nm, _)| nm.clone()).collect();
    let defs: Vec<String> = views.iter().map(|(_, sel)| sel.clone()).collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["epistemic-graph"; n])),
            Arc::new(StringArray::from(vec!["public"; n])),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(defs)),
            Arc::new(StringArray::from(vec!["NONE"; n])),
            Arc::new(StringArray::from(vec!["NO"; n])),
        ],
    )
    .map(|b| (schema, b))
    .map_err(|e| format!("information_schema.views batch: {e}"))
}

/// `information_schema.routines`: one row per durable SQL stored function (CONCEPT:EG-118).
fn info_routines_batch(functions: &[StoredFunction]) -> Result<(SchemaRef, RecordBatch), String> {
    use crate::tables::FunctionReturns;
    let schema = Arc::new(Schema::new(vec![
        Field::new("routine_catalog", DataType::Utf8, false),
        Field::new("routine_schema", DataType::Utf8, false),
        Field::new("routine_name", DataType::Utf8, false),
        Field::new("routine_type", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("routine_definition", DataType::Utf8, false),
        Field::new("external_language", DataType::Utf8, false),
    ]));
    let n = functions.len();
    let names: Vec<String> = functions.iter().map(|f| f.name.clone()).collect();
    let dtypes: Vec<&str> = functions
        .iter()
        .map(|f| match &f.returns {
            FunctionReturns::Scalar(t) => oid_to_sql_type(sql_type_name_to_oid(t)).0,
            FunctionReturns::Table(_) | FunctionReturns::SetOf(_) => "record",
        })
        .collect();
    let defs: Vec<String> = functions.iter().map(|f| f.body.clone()).collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["epistemic-graph"; n])),
            Arc::new(StringArray::from(vec!["public"; n])),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(vec!["FUNCTION"; n])),
            Arc::new(StringArray::from(dtypes)),
            Arc::new(StringArray::from(defs)),
            Arc::new(StringArray::from(vec!["SQL"; n])),
        ],
    )
    .map(|b| (schema, b))
    .map_err(|e| format!("information_schema.routines batch: {e}"))
}

/// `information_schema.key_column_usage`: shaped but empty (minimal, CONCEPT:EG-103).
/// PK/UNIQUE constraint metadata lives in the redb table store (not threaded into the
/// per-query catalog build); an empty-but-shaped table keeps a reflect query running.
fn info_key_column_usage_batch() -> Result<(SchemaRef, RecordBatch), String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("constraint_catalog", DataType::Utf8, false),
        Field::new("constraint_schema", DataType::Utf8, false),
        Field::new("constraint_name", DataType::Utf8, false),
        Field::new("table_catalog", DataType::Utf8, false),
        Field::new("table_schema", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("ordinal_position", DataType::Int32, false),
    ]));
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
        ],
    )
    .map(|b| (schema, b))
    .map_err(|e| format!("information_schema.key_column_usage batch: {e}"))
}

/// `information_schema.table_constraints`: shaped but empty (minimal, CONCEPT:EG-103).
fn info_table_constraints_batch() -> Result<(SchemaRef, RecordBatch), String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("constraint_catalog", DataType::Utf8, false),
        Field::new("constraint_schema", DataType::Utf8, false),
        Field::new("constraint_name", DataType::Utf8, false),
        Field::new("table_catalog", DataType::Utf8, false),
        Field::new("table_schema", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("constraint_type", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ],
    )
    .map(|b| (schema, b))
    .map_err(|e| format!("information_schema.table_constraints batch: {e}"))
}

// ── catalog scalar functions ──────────────────────────────────────────────────

/// A zero-argument constant-string scalar function (`version()` / `current_schema()` /
/// `current_database()` / `current_user`). Implemented as a `ScalarUDFImpl` so
/// DataFusion's zero-arity call path returns the fixed value.
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
    fn invoke_batch(&self, _args: &[ColumnarValue], number_rows: usize) -> DfResult<ColumnarValue> {
        let rows = number_rows.max(1);
        let arr: ArrayRef = Arc::new(StringArray::from(vec![self.value.clone(); rows]));
        Ok(ColumnarValue::Array(arr))
    }
}

/// Build a constant-string catalog function as a registrable [`ScalarUDF`].
fn const_string_udf(name: &str, value: &str) -> ScalarUDF {
    ScalarUDF::from(ConstStringUdf::new(name, value))
}

/// A variadic catalog helper that returns a constant TEXT value (empty string or NULL)
/// regardless of its arguments — for `pg_get_expr`/`pg_get_userbyid`/
/// `pg_get_constraintdef`/`obj_description` and friends, whose full fidelity (real
/// defaults/ACLs/comments) the engine does not model. Accepts any arg count/types so a
/// 1- or 2-arg reflect call resolves (CONCEPT:EG-103).
#[derive(Debug)]
struct ConstTextUdf {
    name: String,
    /// `Some(s)` → return that string; `None` → return SQL NULL.
    value: Option<String>,
    signature: Signature,
}

impl ConstTextUdf {
    fn new(name: &str, value: Option<&str>) -> Self {
        Self {
            name: name.to_string(),
            value: value.map(|s| s.to_string()),
            // Accept any args (1..n) OR zero args, so every reflect-call arity resolves.
            signature: Signature::one_of(
                vec![TypeSignature::VariadicAny, TypeSignature::Any(0)],
                Volatility::Stable,
            ),
        }
    }
}

impl ScalarUDFImpl for ConstTextUdf {
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
    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> DfResult<ColumnarValue> {
        let n = args
            .iter()
            .map(|a| match a {
                ColumnarValue::Array(arr) => arr.len(),
                ColumnarValue::Scalar(_) => 1,
            })
            .max()
            .unwrap_or(number_rows)
            .max(1);
        let arr: ArrayRef = Arc::new(StringArray::from(vec![self.value.clone(); n]));
        Ok(ColumnarValue::Array(arr))
    }
}

/// `pg_catalog.pg_table_is_visible(oid) -> bool` (CONCEPT:EG-103). psql's `\d` filters
/// relations by search-path visibility; the engine has a single visible `public` schema,
/// so every relation is visible → always `true`. Variadic-any so any oid arg type binds.
#[derive(Debug)]
struct PgTableIsVisibleUdf {
    signature: Signature,
}

impl Default for PgTableIsVisibleUdf {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::VariadicAny, TypeSignature::Any(0)],
                Volatility::Stable,
            ),
        }
    }
}

impl ScalarUDFImpl for PgTableIsVisibleUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "pg_table_is_visible"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> DfResult<ColumnarValue> {
        let n = args
            .iter()
            .map(|a| match a {
                ColumnarValue::Array(arr) => arr.len(),
                ColumnarValue::Scalar(_) => 1,
            })
            .max()
            .unwrap_or(number_rows)
            .max(1);
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![true; n]));
        Ok(ColumnarValue::Array(arr))
    }
}

/// `format_type(oid, typmod) -> text` (CONCEPT:EG-103). Maps the type OID in the FIRST
/// argument to its pg type name (ignoring `typmod`, whose length/precision detail the
/// engine's coarse types don't carry). Variadic so a 1- or 2-arg call resolves.
#[derive(Debug)]
struct FormatTypeUdf {
    signature: Signature,
}

impl Default for FormatTypeUdf {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for FormatTypeUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "format_type"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> DfResult<ColumnarValue> {
        use arrow::compute::cast;
        let arrays = ColumnarValue::values_to_arrays(args)?;
        let n = arrays
            .iter()
            .map(|a| a.len())
            .max()
            .unwrap_or(number_rows)
            .max(1);
        // First arg is the type OID; cast to Int32 so any integer/typed oid input reads.
        let oid_col = arrays.first().and_then(|a| cast(a, &DataType::Int32).ok());
        let out: StringArray = (0..n)
            .map(|i| match &oid_col {
                Some(a) => {
                    let a = a.as_any().downcast_ref::<Int32Array>();
                    match a {
                        Some(a) if !a.is_null(i.min(a.len().saturating_sub(1))) => {
                            let idx = i.min(a.len().saturating_sub(1));
                            Some(pg_typname(a.value(idx)).to_string())
                        }
                        _ => Some("text".to_string()),
                    }
                }
                None => Some("text".to_string()),
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
    }
}

/// Register a `MemTable` (one `(schema, batch)`) under a synthetic schema provider.
fn register_syscat_table(
    provider: &Arc<MemorySchemaProvider>,
    name: &str,
    table: (SchemaRef, RecordBatch),
) -> Result<(), String> {
    let mem = MemTable::try_new(table.0, vec![vec![table.1]])
        .map_err(|e| format!("{name} memtable: {e}"))?;
    provider
        .register_table(name.to_string(), Arc::new(mem))
        .map_err(|e| format!("register {name}: {e}"))?;
    Ok(())
}

/// Register the synthetic `pg_catalog` + `information_schema` system views and the
/// catalog scalar functions into `ctx` (CONCEPT:EG-103, extending CONCEPT:KG-2.201),
/// synthesized from the live relations (`nodes`/`edges`/user tables), the durable views,
/// and the EG-118 SQL functions. Called ONCE per built context, AFTER the durable views
/// are registered (so views appear with their real columns). DataFusion's native
/// `information_schema` is disabled by the exec builder; this fully replaces it.
pub(crate) async fn register_system_catalogs(
    ctx: &SessionContext,
    nodes_schema: &SchemaRef,
    edges_schema: &SchemaRef,
    user_relations: &[(String, SchemaRef)],
    views: &[(String, String)],
    functions: &[StoredFunction],
) -> Result<(), String> {
    let rels = collect_relations(ctx, nodes_schema, edges_schema, user_relations, views).await;

    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(|| "default catalog 'datafusion' missing".to_string())?;

    // pg_catalog.*
    let pg_schema = Arc::new(MemorySchemaProvider::new());
    register_syscat_table(&pg_schema, "pg_namespace", pg_namespace_batch()?)?;
    register_syscat_table(&pg_schema, "pg_class", pg_class_batch(&rels)?)?;
    register_syscat_table(&pg_schema, "pg_attribute", pg_attribute_batch(&rels)?)?;
    register_syscat_table(&pg_schema, "pg_type", pg_type_batch()?)?;
    register_syscat_table(&pg_schema, "pg_index", pg_index_batch()?)?;
    register_syscat_table(&pg_schema, "pg_proc", pg_proc_batch(functions)?)?;
    register_syscat_table(&pg_schema, "pg_database", pg_database_batch()?)?;
    register_syscat_table(&pg_schema, "pg_settings", pg_settings_batch()?)?;
    catalog
        .register_schema("pg_catalog", pg_schema)
        .map_err(|e| format!("register pg_catalog schema: {e}"))?;

    // information_schema.*
    let info_schema = Arc::new(MemorySchemaProvider::new());
    register_syscat_table(&info_schema, "schemata", info_schemata_batch()?)?;
    register_syscat_table(&info_schema, "tables", info_tables_batch(&rels)?)?;
    register_syscat_table(&info_schema, "columns", info_columns_batch(&rels)?)?;
    register_syscat_table(&info_schema, "views", info_views_batch(views)?)?;
    register_syscat_table(&info_schema, "routines", info_routines_batch(functions)?)?;
    register_syscat_table(
        &info_schema,
        "key_column_usage",
        info_key_column_usage_batch()?,
    )?;
    register_syscat_table(
        &info_schema,
        "table_constraints",
        info_table_constraints_batch()?,
    )?;
    catalog
        .register_schema("information_schema", info_schema)
        .map_err(|e| format!("register information_schema schema: {e}"))?;

    // Catalog scalar functions drivers probe on connect / while reflecting.
    ctx.register_udf(const_string_udf(
        "version",
        "PostgreSQL 16.6 (epistemic-graph pgwire) on x86_64-epistemic, compiled by rustc",
    ));
    ctx.register_udf(const_string_udf("current_schema", "public"));
    ctx.register_udf(const_string_udf("current_database", "epistemic-graph"));
    ctx.register_udf(const_string_udf("current_user", "epistemic"));
    ctx.register_udf(const_string_udf("session_user", "epistemic"));
    // Reflect helpers — sensible constants where full fidelity isn't feasible.
    ctx.register_udf(ScalarUDF::from(PgTableIsVisibleUdf::default()));
    ctx.register_udf(ScalarUDF::from(FormatTypeUdf::default()));
    ctx.register_udf(ScalarUDF::from(ConstTextUdf::new("obj_description", None)));
    ctx.register_udf(ScalarUDF::from(ConstTextUdf::new(
        "col_description",
        None,
    )));
    ctx.register_udf(ScalarUDF::from(ConstTextUdf::new(
        "shobj_description",
        None,
    )));
    ctx.register_udf(ScalarUDF::from(ConstTextUdf::new("pg_get_expr", Some(""))));
    ctx.register_udf(ScalarUDF::from(ConstTextUdf::new(
        "pg_get_userbyid",
        Some("epistemic"),
    )));
    ctx.register_udf(ScalarUDF::from(ConstTextUdf::new(
        "pg_get_constraintdef",
        Some(""),
    )));
    ctx.register_udf(ScalarUDF::from(ConstTextUdf::new(
        "pg_get_indexdef",
        Some(""),
    )));

    Ok(())
}
