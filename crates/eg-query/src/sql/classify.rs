//! Pure parse-to-classify helper for the Postgres wire shim (CONCEPT:KG-2.189,
//! DML completeness CONCEPT:KG-2.198).
//!
//! A SQL statement arriving over the pgwire surface must be routed by KIND
//! *before* it is executed: a `SELECT` is a read (it reuses the DataFusion
//! `exec_sql` path, exactly as `Method::Sql` does); an `INSERT`/`UPDATE`/`DELETE`
//! is a write and must go through the engine's GraphTxn write path, NOT
//! DataFusion's awkward write planner. This module does ONLY the classification
//! (a pure function over the parsed AST) so it is unit-testable without a graph,
//! a runtime, or a socket.
//!
//! It reuses the SAME parser the rest of the SQL surface uses — `sqlparser`
//! re-exported by `datafusion::sql` — so there is no second SQL grammar in the
//! tree and a statement that parses here parses identically downstream.
//!
//! ## DML shapes supported (CONCEPT:KG-2.198)
//! Over the `nodes` table only (the graph's node store):
//!   * `INSERT INTO nodes (id, …) VALUES (…)[, (…)…]` — single OR multi-row.
//!   * `UPDATE nodes SET k = v[, …] WHERE id = '…'` — also a simple equality WHERE
//!     on ANY single property column (`WHERE <prop> = <literal>`), which selects
//!     every node whose current value of `<prop>` equals the literal.
//!   * `DELETE FROM nodes WHERE id = '…'` — same simple-WHERE shapes as UPDATE.
//!   * `RETURNING …` on INSERT/UPDATE/DELETE — captured as a flag; the shim turns
//!     the affected nodes into a result set after the write.
//!
//! Parameterized values (`$1`, `$2`, …) coming from the extended protocol are
//! substituted to SQL literals by the shim BEFORE `classify` runs, so classify
//! only ever sees literals and stays a pure data move with no evaluation.
//!
//! ## Deferred (explicit follow-ups, rejected with a precise error)
//!   * Complex WHERE (`AND`/`OR`/ranges/`IN`) in UPDATE/DELETE.
//!   * Joins, subqueries, or `FROM` clauses in UPDATE/DELETE.
//!   * `INSERT … SELECT`, `ON CONFLICT`, expressions/functions in VALUES.
//!   * Writes to any table other than `nodes`.

use datafusion::sql::sqlparser::ast::{
    AlterTableOperation, Assignment, AssignmentTarget, BinaryOperator, ColumnDef as SqlColumnDef,
    ColumnOption, ConflictTarget, CopyLegacyOption, CopyOption, CopySource, CopyTarget, CreateTable,
    Delete, Expr, FromTable, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Insert,
    ObjectName, ObjectType, OnConflictAction as SqlOnConflictAction, OnInsert, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins, UnaryOperator, Value as SqlValue, Values,
};
// CONCEPT:EG-114/116/117 — the Postgres-family extension plan shapes classify routes to.
use super::pgfamily::{AnnIndexPlan, ContinuousAggPlan, CypherCallPlan, HypertablePlan};
// CONCEPT:EG-084 — the wire predicate the JSON operators lower onto. Surfaced by
// `eg-types/query`, which the `sql` feature (this module's gate) always enables.
use eg_types::wire::{JsonPathOp, Pred};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use serde_json::{Map, Value};

use crate::tables::schema::{
    CmpOp, ColCheck, FunctionArg as CatalogArg, FunctionReturns, StoredFunction,
};

/// How a single parsed SQL statement should be routed by the wire shim.
#[derive(Debug, Clone, PartialEq)]
pub enum StatementKind {
    /// A read (`SELECT`/`WITH`/`SHOW`/`EXPLAIN`). Reuse the DataFusion `exec_sql`
    /// path over an off-lock snapshot — identical to `Method::Sql`.
    Read,
    /// `INSERT INTO nodes (...) VALUES (...)[, (...)…]` — one or more node
    /// creations, fully decoded into id + property objects so they can be applied
    /// through a `GraphTxn`. A single-row INSERT is just a one-element vector.
    InsertNodes(InsertNodes),
    /// `INSERT INTO nodes (id, …) SELECT …` (CONCEPT:EG-046) — populate the node
    /// store from the projected rows of a SELECT (which may itself JOIN user tables
    /// and the graph). The SELECT text is re-run through the DataFusion read path;
    /// each result row builds an id + property object applied like `InsertNodes`.
    InsertNodesSelect(InsertNodesSelect),
    /// `UPDATE nodes SET k = v[, …] WHERE …` — decoded SET map + a simple WHERE
    /// predicate, routed to `compare_and_set_fields` per matched node under a txn.
    UpdateNodes(UpdateNodes),
    /// `UPDATE nodes SET … FROM <other> WHERE …` (CONCEPT:EG-047) — a correlated,
    /// multi-table update: the matched ids AND per-row SET values are resolved
    /// through DataFusion (`resolve_sql` yields `(id, <set-cols…>)`), then applied
    /// per node via the serializable CAS gate.
    UpdateNodesJoin(UpdateNodesJoin),
    /// `DELETE FROM nodes WHERE …` — a simple WHERE predicate, routed to
    /// `remove_node` per matched node under a txn.
    DeleteNodes(DeleteNodes),
    /// `DELETE FROM nodes USING <other> WHERE …` (CONCEPT:EG-047) — a correlated,
    /// multi-table delete: the matched ids are resolved through DataFusion
    /// (`resolve_sql` yields `id`), then each is removed under its one-shot txn.
    DeleteNodesJoin(DeleteNodesJoin),

    // ── arbitrary user-defined relational tables (CONCEPT:EG-018) ──────────────
    /// `CREATE TABLE name (col type, …) [IF NOT EXISTS]` — a new user table whose
    /// schema is recorded in the redb table catalog (NOT the graph projection).
    CreateTable(CreateTablePlan),
    /// `DROP TABLE [IF EXISTS] name` — remove a user table and all its rows.
    DropTable(DropTablePlan),
    /// `ALTER TABLE name ADD COLUMN col type` — append a column to a user table.
    AlterTable(AlterTablePlan),
    /// `CREATE VIEW name AS <select>` (CONCEPT:EG-072) — record a read-only named
    /// query in the durable view catalog; a later SELECT that references it expands
    /// the stored SELECT during context build.
    CreateView(CreateViewPlan),
    /// `DROP VIEW [IF EXISTS] name` (CONCEPT:EG-072) — remove a view catalog entry.
    DropView(DropViewPlan),
    /// `INSERT INTO <user_table> (cols…) VALUES (…)[, …]` — literal multi-row insert
    /// into a user table (not `nodes`).
    InsertTable(InsertTable),
    /// `INSERT INTO <user_table> (cols…) SELECT …` — insert the projected rows of a
    /// SELECT (which may itself JOIN user tables AND the graph) into a user table.
    InsertSelect(InsertSelect),
    /// `UPDATE <user_table> SET … WHERE <col> = <literal>` — typed update of a user table.
    UpdateTable(UpdateTable),
    /// `DELETE FROM <user_table> WHERE <col> = <literal>` — typed delete from a user table.
    DeleteTable(DeleteTable),

    // ── transactions + bulk ingest (CONCEPT:EG-020) ────────────────────────────
    /// `BEGIN` / `START TRANSACTION` — open a multi-statement transaction.
    Begin,
    /// `COMMIT` — apply the open transaction's buffered ops in one redb txn.
    Commit,
    /// `ROLLBACK` — discard the open transaction.
    Rollback,
    /// `COPY <table> [(cols…)] FROM STDIN [WITH (FORMAT …)]` — bulk ingest; the shim
    /// switches the connection into copy-in mode and streams rows into the user table.
    CopyIn(CopyPlan),

    // ── extensions (CONCEPT:EG-102) ────────────────────────────────────────────
    /// `CREATE EXTENSION [IF NOT EXISTS] name [WITH SCHEMA …]` — record `name` in the
    /// durable extension catalog so a client's setup script proceeds. The concrete
    /// surface each extension unlocks (pgvector types/ops, AGE, TimescaleDB, pg_search)
    /// lands in its own later item; this accepts + records the enablement.
    CreateExtension {
        name: String,
        if_not_exists: bool,
    },
    /// `DROP EXTENSION [IF EXISTS] name [CASCADE|RESTRICT]` — remove a catalog entry.
    DropExtension {
        name: String,
        if_exists: bool,
    },

    // ── Postgres-family extension parity (wave 19) ─────────────────────────────
    /// `SELECT <proj> FROM cypher('graph', $$ <cypher> $$) AS (cols…)` (CONCEPT:EG-114)
    /// — an Apache-AGE set-returning function. The inner Cypher runs on the named
    /// graph; its agtype (JSON) result is projected onto the `AS` columns.
    CypherCall(CypherCallPlan),
    /// `CREATE INDEX … USING hnsw|ivfflat (col opclass)` (CONCEPT:EG-116) — register a
    /// pgvector ANN index so a `ORDER BY col <-> $1 LIMIT k` query pushes down to eg-ann.
    CreateAnnIndex(AnnIndexPlan),
    /// `SELECT create_hypertable('t','ts')` (CONCEPT:EG-117) — record TimescaleDB
    /// time-partitioning metadata for a table.
    CreateHypertable(HypertablePlan),
    /// `CREATE MATERIALIZED VIEW … WITH (timescaledb.continuous) AS SELECT …`
    /// (CONCEPT:EG-117) — a continuous aggregate lowered onto the durable view catalog.
    CreateContinuousAggregate(ContinuousAggPlan),

    // ── SQL stored functions (CONCEPT:EG-118) ───────────────────────────────────
    /// `CREATE [OR REPLACE] FUNCTION name(arg type, …) RETURNS … AS $$ … $$ LANGUAGE sql`
    /// — record a SQL-language stored function in the durable function catalog. A later
    /// `SELECT fn(args)` (scalar) inlines the body expression, and a `FROM fn(args)`
    /// (`RETURNS TABLE`/`SETOF`) expands the body as a parameterized-view subquery.
    CreateFunction(CreateFunctionPlan),
    /// `DROP FUNCTION [IF EXISTS] name [(…)]` (CONCEPT:EG-118) — remove a function.
    DropFunction(DropFunctionPlan),
}

/// A decoded `COPY <table> [(cols…)] FROM STDIN` (CONCEPT:EG-020). `columns` empty ⇒
/// all columns in schema order. `format` selects the row decoder applied to the
/// streamed `CopyData` bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyPlan {
    pub table: String,
    pub columns: Vec<String>,
    pub format: CopyFormat,
    /// Custom field delimiter (TEXT/CSV); defaults per format (`\t` text, `,` csv).
    pub delimiter: Option<char>,
    /// Whether a header line precedes the data (CSV `HEADER`).
    pub header: bool,
}

/// The wire format of a `COPY … FROM STDIN` body (CONCEPT:EG-020).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    /// Postgres TEXT format: tab-delimited, `\N` is NULL.
    Text,
    /// CSV format: comma-delimited, quoted fields, empty unquoted = NULL.
    Csv,
    /// Postgres BINARY format (signature header + length-prefixed fields).
    Binary,
}

/// One column of a `CREATE TABLE` / `ALTER TABLE ADD COLUMN` (CONCEPT:EG-018). The
/// `type_name` is the raw SQL type spelling (e.g. `BIGINT`, `DOUBLE PRECISION`,
/// `TIMESTAMP`); the executor resolves it to a `tables::ColumnType`. `nullable` is
/// false when `NOT NULL` (or `PRIMARY KEY`) was declared; `primary_key` records a
/// column-level `PRIMARY KEY`.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
    pub primary_key: bool,
    /// `UNIQUE` (or `PRIMARY KEY`) (CONCEPT:EG-020).
    pub unique: bool,
    /// `SERIAL`/`BIGSERIAL` or `DEFAULT nextval(...)` — auto-increment (CONCEPT:EG-020).
    pub serial: bool,
    /// `DEFAULT <literal>` value (CONCEPT:EG-020).
    pub default: Option<Value>,
    /// A simple `CHECK (col OP literal)` (CONCEPT:EG-020).
    pub check: Option<ColCheck>,
}

/// A decoded `CREATE TABLE` (CONCEPT:EG-018).
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTablePlan {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub if_not_exists: bool,
}

/// A decoded `DROP TABLE` (CONCEPT:EG-018).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTablePlan {
    pub name: String,
    pub if_exists: bool,
}

/// A decoded `ALTER TABLE … ADD COLUMN` (CONCEPT:EG-018).
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTablePlan {
    pub name: String,
    pub add_column: ColumnDef,
}

/// A compound WHERE on a user table (CONCEPT:EG-045). Unlike [`WhereEq`] there is no
/// `id` special-casing — a user table has no implicit id column. The full predicate
/// is carried as a serializable [`eg_types::RowPredicate`] and evaluated per row by
/// the redb store INSIDE the open write transaction (serializable for free).
#[derive(Debug, Clone, PartialEq)]
pub struct TableWhereEq {
    pub pred: eg_types::RowPredicate,
}

/// A decoded literal `INSERT INTO <user_table> … VALUES …` (CONCEPT:EG-018), with an
/// optional `ON CONFLICT` action and `RETURNING` flag (CONCEPT:EG-048).
#[derive(Debug, Clone, PartialEq)]
pub struct InsertTable {
    pub table: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    /// `ON CONFLICT (...) DO NOTHING|DO UPDATE` (CONCEPT:EG-048), or `None`.
    pub on_conflict: Option<OnConflict>,
    /// Whether a `RETURNING` clause was present (CONCEPT:EG-048).
    pub returning: bool,
}

/// A decoded `INSERT INTO nodes (id, …) SELECT …` (CONCEPT:EG-046). The SELECT text is
/// re-run through the DataFusion read path so it can JOIN user tables and the graph;
/// each projected row becomes a node insert. `columns` must include `id`.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertNodesSelect {
    pub columns: Vec<String>,
    pub select_sql: String,
    pub returning: bool,
    /// `ON CONFLICT` action applied per resolved row (CONCEPT:EG-048).
    pub on_conflict: Option<OnConflict>,
}

/// A decoded `INSERT … ON CONFLICT (target_cols) DO NOTHING|DO UPDATE SET …`
/// (CONCEPT:EG-048). For `nodes` the conflict key is always the node `id` (the
/// `target_cols` are informational); for a user table `target_cols` name the
/// unique/PK columns whose duplicate triggers the action (validated via the store's
/// existing uniqueness check).
#[derive(Debug, Clone, PartialEq)]
pub struct OnConflict {
    pub target_cols: Vec<String>,
    pub action: OnConflictAction,
}

/// The action an `ON CONFLICT` clause takes on a conflicting row (CONCEPT:EG-048).
#[derive(Debug, Clone, PartialEq)]
pub enum OnConflictAction {
    /// `DO NOTHING` — skip the conflicting row.
    DoNothing,
    /// `DO UPDATE SET …` — merge these column assignments into the existing row.
    DoUpdate(Map<String, Value>),
}

/// A decoded `UPDATE nodes SET … FROM <other> WHERE …` (CONCEPT:EG-047). `resolve_sql`
/// is a SELECT that projects `id` plus one column per SET target (aliased to the
/// target column name); the executor reads it, then applies each row's SET values to
/// the node with that id via the serializable CAS gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateNodesJoin {
    pub set_targets: Vec<String>,
    pub resolve_sql: String,
    pub returning: bool,
}

/// A decoded `DELETE FROM nodes USING <other> WHERE …` (CONCEPT:EG-047). `resolve_sql`
/// is a SELECT that projects the `id` of every node to remove.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteNodesJoin {
    pub resolve_sql: String,
    pub returning: bool,
}

/// A decoded `CREATE VIEW name AS <select>` (CONCEPT:EG-072). `select_sql` is the raw
/// SELECT text stored in the durable view catalog; `or_replace` mirrors
/// `CREATE OR REPLACE VIEW`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateViewPlan {
    pub name: String,
    pub select_sql: String,
    pub or_replace: bool,
}

/// A decoded `DROP VIEW [IF EXISTS] name` (CONCEPT:EG-072).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropViewPlan {
    pub name: String,
    pub if_exists: bool,
}

/// A decoded `CREATE [OR REPLACE] FUNCTION …` (CONCEPT:EG-118). `func` is the durable
/// definition persisted in the function catalog; `or_replace` mirrors `OR REPLACE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateFunctionPlan {
    pub func: StoredFunction,
    pub or_replace: bool,
}

/// A decoded `DROP FUNCTION [IF EXISTS] name [(…)]` (CONCEPT:EG-118). Functions are
/// keyed by name (overloading by argument-type signature is a documented follow-up), so
/// the argument-type list — if present — is parsed and ignored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropFunctionPlan {
    pub name: String,
    pub if_exists: bool,
}

/// A decoded `INSERT INTO <user_table> (cols…) SELECT …` (CONCEPT:EG-018). The SELECT
/// is kept as text and run through the SAME DataFusion path (so it can JOIN user
/// tables and the graph), and its rows are then inserted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertSelect {
    pub table: String,
    pub columns: Vec<String>,
    pub select_sql: String,
}

/// A decoded `UPDATE <user_table> SET … WHERE <col> = <literal>` (CONCEPT:EG-018).
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateTable {
    pub table: String,
    pub set: Map<String, Value>,
    pub selector: TableWhereEq,
}

/// A decoded `DELETE FROM <user_table> WHERE <col> = <literal>` (CONCEPT:EG-018).
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteTable {
    pub table: String,
    pub selector: TableWhereEq,
}

/// A simple single-column equality predicate, the only WHERE shape the wire DML
/// path resolves. `WHERE id = '…'` is the fast path (one node by id); any other
/// `WHERE <prop> = <literal>` selects every node whose `<prop>` equals the literal.
#[derive(Debug, Clone, PartialEq)]
pub enum WhereEq {
    /// `WHERE id = <value>` — addresses exactly one node by its id (fast path).
    Id(String),
    /// Any other WHERE (CONCEPT:EG-045): a compound `AND`/`OR`/`NOT`/`IN`/`BETWEEN`/
    /// range/`IS [NOT] NULL` predicate, OR a single `<prop> = <literal>`. `where_sql`
    /// is the predicate text (`expr.to_string()`) the shim re-runs through the
    /// DataFusion read path to resolve candidate ids; `pred` is the serializable AST
    /// the engine re-checks under the write guard for serializable semantics.
    Predicate {
        where_sql: String,
        pred: eg_types::RowPredicate,
    },
}

/// A decoded single-node row for an `INSERT INTO nodes (id, …) VALUES (…)`.
/// `node_id` is the value of the `id` column; `properties` are the remaining
/// columns as a JSON object (the same shape the AddNode write path stores as a
/// MessagePack blob).
#[derive(Debug, Clone, PartialEq)]
pub struct InsertNode {
    pub node_id: String,
    pub properties: Map<String, Value>,
}

/// One or more decoded `INSERT` rows plus whether a `RETURNING` clause was present,
/// and an optional `ON CONFLICT` action applied per row (CONCEPT:EG-048).
#[derive(Debug, Clone, PartialEq)]
pub struct InsertNodes {
    pub rows: Vec<InsertNode>,
    pub returning: bool,
    pub on_conflict: Option<OnConflict>,
}

/// A decoded `UPDATE nodes SET … WHERE …`: the property updates to merge and the
/// matched node selector. `returning` mirrors a `RETURNING` clause.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateNodes {
    pub set: Map<String, Value>,
    pub selector: WhereEq,
    pub returning: bool,
}

/// A decoded `DELETE FROM nodes WHERE …`: the matched node selector + `RETURNING`.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteNodes {
    pub selector: WhereEq,
    pub returning: bool,
}

/// Parse `sql` (one statement) with the Postgres dialect and classify it.
///
/// Pure: no graph, no runtime, no I/O. Returns an `Err(String)` for an empty
/// batch, multiple statements (the shim handles one per call), a parse error, or
/// a write whose shape this increment cannot route (e.g. a write into a table
/// other than `nodes`, a complex WHERE, or a join/subquery in DML).
pub fn classify(sql: &str) -> Result<StatementKind, String> {
    // CONCEPT:EG-102 — `DROP EXTENSION` has no `sqlparser` AST node (no
    // `ObjectType::Extension`), so recognize it textually BEFORE the parser (mirrors
    // the `COPY … FROM STDIN` pre-check) and route it to the extension catalog.
    if let Some((name, if_exists)) = parse_drop_extension(sql) {
        return Ok(StatementKind::DropExtension { name, if_exists });
    }
    // CONCEPT:EG-118 — `CREATE [OR REPLACE] FUNCTION … LANGUAGE sql` and `DROP FUNCTION`.
    // The dollar-quoted `$$ … $$` body + the typed argument/`RETURNS TABLE(...)` lists do
    // not round-trip through `sqlparser` 0.51's `CreateFunction` AST cleanly, so — exactly
    // like AGE `cypher()` / `DROP EXTENSION` — the shape is recognized TEXTUALLY before the
    // parser. A malformed `CREATE FUNCTION` returns a precise `Err` (never silently mis-
    // routed to the parser, which would emit a confusing message).
    if is_create_function(sql) {
        return parse_create_function(sql).map(StatementKind::CreateFunction);
    }
    if is_drop_function(sql) {
        return parse_drop_function(sql).map(StatementKind::DropFunction);
    }
    // CONCEPT:EG-114 — Apache AGE `cypher('g', $$ … $$) AS (cols…)`. `sqlparser` 0.51
    // cannot parse the typed `AS` column list on a table function, so recognize it
    // textually before the parser (like `DROP EXTENSION`) and route to the Cypher engine.
    if let Some(plan) = super::pgfamily::parse_cypher_call(sql) {
        return Ok(StatementKind::CypherCall(plan));
    }
    // CONCEPT:EG-116 — pgvector `CREATE INDEX … USING hnsw|ivfflat (col opclass)`. The
    // opclass (and `IF NOT EXISTS` on an index) does not parse in `sqlparser` 0.51, so
    // recognize the ANN-index shape textually. A non-ANN `CREATE INDEX` returns `None`.
    if let Some(plan) = super::pgfamily::parse_create_ann_index(sql) {
        return Ok(StatementKind::CreateAnnIndex(plan));
    }
    // CONCEPT:EG-117 — TimescaleDB continuous aggregate. The dotted
    // `WITH (timescaledb.continuous)` option does not parse, so recognize it textually;
    // a plain `CREATE MATERIALIZED VIEW` returns `None` and the parser rejects it (a
    // documented follow-up in `classify_create_view`).
    if let Some(plan) = super::pgfamily::parse_continuous_aggregate(sql) {
        return Ok(StatementKind::CreateContinuousAggregate(plan));
    }
    // `COPY … FROM STDIN` is sent over the wire WITHOUT the inline TSV data block that
    // `sqlparser` insists follows the `;` — so append a `;` to satisfy its grammar
    // (it then parses an EMPTY data block). The real rows arrive as `CopyData` frames.
    let owned;
    let to_parse: &str = if is_copy_from_stdin(sql) {
        owned = format!("{};", sql.trim_end().trim_end_matches(';'));
        &owned
    } else {
        sql
    };
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, to_parse)
        .map_err(|e| format!("parse error: {e}"))?;
    let stmt = match stmts.as_slice() {
        [s] => s,
        [] => return Err("empty statement".to_string()),
        _ => return Err("multiple statements per query are not supported".to_string()),
    };

    match stmt {
        // CONCEPT:EG-117 — `SELECT create_hypertable('t','ts')` parses as an ordinary
        // query; detect it before falling through to the read path.
        Statement::Query(_) => {
            if let Some(plan) = super::pgfamily::detect_create_hypertable(stmt) {
                return Ok(StatementKind::CreateHypertable(plan));
            }
            Ok(StatementKind::Read)
        }
        Statement::Explain { .. }
        | Statement::ShowVariable { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowTables { .. } => Ok(StatementKind::Read),
        Statement::Insert(insert) => classify_any_insert(insert),
        Statement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
        } => classify_any_update(
            table,
            assignments,
            from.as_ref(),
            selection.as_ref(),
            returning,
        ),
        Statement::Delete(delete) => classify_any_delete(delete),
        // ── DDL (CONCEPT:EG-018) ──────────────────────────────────────────────
        Statement::CreateTable(ct) => classify_create_table(ct).map(StatementKind::CreateTable),
        // ── extensions (CONCEPT:EG-102) ─────────────────────────────────────────
        Statement::CreateExtension {
            name,
            if_not_exists,
            ..
        } => classify_create_extension(&name.value, *if_not_exists),
        Statement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => classify_drop(*object_type, *if_exists, names),
        Statement::AlterTable {
            name, operations, ..
        } => classify_alter_table(name, operations).map(StatementKind::AlterTable),
        // ── views (CONCEPT:EG-072) ─────────────────────────────────────────────
        Statement::CreateView {
            name,
            query,
            or_replace,
            materialized,
            ..
        } => classify_create_view(name, query, *or_replace, *materialized),
        // ── transactions + COPY (CONCEPT:EG-020) ──────────────────────────────
        Statement::StartTransaction { .. } => Ok(StatementKind::Begin),
        Statement::Commit { .. } => Ok(StatementKind::Commit),
        Statement::Rollback { .. } => Ok(StatementKind::Rollback),
        Statement::Copy {
            source,
            to,
            target,
            options,
            legacy_options,
            ..
        } => classify_copy(source, *to, target, options, legacy_options),
        other => Err(format!("unsupported statement: {other}")),
    }
}

/// A lightweight check that `sql` is a `COPY … FROM STDIN` (so `classify` can append
/// the `;` sqlparser's grammar requires). Conservative: leading keyword `COPY` and the
/// phrase `FROM STDIN` present (case-insensitive), outside the concern of exact options.
fn is_copy_from_stdin(sql: &str) -> bool {
    let up = sql.trim_start().to_ascii_uppercase();
    up.starts_with("COPY ") && up.contains("FROM STDIN")
}

/// Decode `COPY <table> [(cols…)] FROM STDIN [WITH (FORMAT csv|binary|text), …]`
/// (CONCEPT:EG-020). Only `FROM STDIN` into a user table is accepted; `COPY TO`,
/// `COPY (query)`, and `COPY … FROM 'file'`/`PROGRAM` are rejected (no server-side
/// filesystem access over the wire).
fn classify_copy(
    source: &CopySource,
    to: bool,
    target: &CopyTarget,
    options: &[CopyOption],
    legacy_options: &[CopyLegacyOption],
) -> Result<StatementKind, String> {
    if to {
        return Err("COPY TO is not supported (only COPY … FROM STDIN)".to_string());
    }
    if !matches!(target, CopyTarget::Stdin) {
        return Err("COPY supports only FROM STDIN (no file/program source)".to_string());
    }
    let (table, columns) = match source {
        CopySource::Table {
            table_name,
            columns,
        } => {
            let leaf = last_ident(table_name);
            if is_reserved_table(&leaf) {
                return Err(format!(
                    "COPY cannot target the reserved graph table `{leaf}`"
                ));
            }
            (leaf, columns.iter().map(|c| c.value.clone()).collect())
        }
        CopySource::Query(_) => {
            return Err("COPY (query) FROM is not valid; use COPY <table> FROM STDIN".to_string())
        }
    };

    // Resolve the format + delimiter + header from BOTH the modern `WITH (...)` options
    // and the legacy positional options.
    let mut format = CopyFormat::Text;
    let mut delimiter = None;
    let mut header = false;
    for opt in options {
        match opt {
            CopyOption::Format(name) => {
                format = match name.value.to_ascii_lowercase().as_str() {
                    "csv" => CopyFormat::Csv,
                    "binary" => CopyFormat::Binary,
                    "text" => CopyFormat::Text,
                    other => return Err(format!("unsupported COPY format `{other}`")),
                };
            }
            CopyOption::Delimiter(c) => delimiter = Some(*c),
            CopyOption::Header(h) => header = *h,
            _ => {}
        }
    }
    for opt in legacy_options {
        match opt {
            CopyLegacyOption::Binary => format = CopyFormat::Binary,
            CopyLegacyOption::Csv(_) if format == CopyFormat::Text => {
                format = CopyFormat::Csv;
            }
            CopyLegacyOption::Delimiter(c) => delimiter = Some(*c),
            _ => {}
        }
    }
    Ok(StatementKind::CopyIn(CopyPlan {
        table,
        columns,
        format,
        delimiter,
        header,
    }))
}

/// Where a `$N` parameter placeholder appears, for type inference in the extended
/// protocol's Describe step (CONCEPT:KG-2.197). The shim can't statically know a
/// column's type, so it resolves `Column(name)` against the inferred node schema;
/// `IdColumn` is always TEXT; `Literal(_)` carries the directly-derivable type.
#[derive(Debug, Clone, PartialEq)]
pub enum ParamSite {
    /// The param is compared/assigned against the `id` column → TEXT.
    IdColumn,
    /// The param is compared/assigned against this property column → resolve its
    /// type from the inferred node schema.
    Column(String),
    /// The param's type is directly derivable (it sits opposite a literal in a
    /// comparison, or no context was found) — a coarse hint.
    Literal(ParamLiteralType),
}

/// A coarse param type derivable without the node schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamLiteralType {
    Int,
    Float,
    Bool,
    Text,
}

/// Infer, for each `$N` placeholder (index `N-1` in the returned vector), WHERE it
/// is used so the extended-protocol Describe step can report a usable parameter
/// type (CONCEPT:KG-2.197). Pure: parses the SQL and walks the relevant clauses
/// (`SET k = $n`, `WHERE col = $n` / `col OP $n`, `VALUES (…, $n, …)` against the
/// insert column list). A param with no resolvable context defaults to
/// `Literal(Text)`. The vector length is the max `$N` seen (dense, 1-based).
///
/// This does NOT resolve a column's concrete type (the shim does, against the
/// inferred node schema) — it only locates each param so the shim knows which
/// column/`id`/literal to type it from. Kept here so it shares the SAME parser as
/// `classify` and stays unit-testable.
pub fn infer_param_sites(sql: &str) -> Result<Vec<ParamSite>, String> {
    let stmts =
        Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|e| format!("parse error: {e}"))?;
    let stmt = match stmts.as_slice() {
        [s] => s,
        [] => return Err("empty statement".to_string()),
        _ => return Err("multiple statements per query are not supported".to_string()),
    };
    // Collect (param_index, site); the highest index sets the vector length.
    let mut sites: std::collections::HashMap<usize, ParamSite> = std::collections::HashMap::new();
    match stmt {
        Statement::Query(q) => collect_query_param_sites(q, &mut sites),
        Statement::Update {
            assignments,
            selection,
            ..
        } => {
            for a in assignments {
                if let AssignmentTarget::ColumnName(name) = &a.target {
                    record_value_site(&last_ident(name), &a.value, &mut sites);
                }
            }
            if let Some(sel) = selection {
                collect_expr_param_sites(sel, &mut sites);
            }
        }
        Statement::Delete(delete) => {
            if let Some(sel) = &delete.selection {
                collect_expr_param_sites(sel, &mut sites);
            }
        }
        Statement::Insert(insert) => {
            let columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();
            if let Some(src) = &insert.source {
                if let SetExpr::Values(Values { rows, .. }) = src.body.as_ref() {
                    for row in rows {
                        for (col, expr) in columns.iter().zip(row.iter()) {
                            record_value_site(col, expr, &mut sites);
                        }
                    }
                }
            }
        }
        _ => {}
    }
    let max_n = sites.keys().copied().max().unwrap_or(0);
    let mut out = vec![ParamSite::Literal(ParamLiteralType::Text); max_n];
    for (idx, site) in sites {
        if idx >= 1 && idx <= max_n {
            out[idx - 1] = site;
        }
    }
    Ok(out)
}

/// Record the site of a `$N` whose VALUE is `expr` set/inserted into `column`.
fn record_value_site(
    column: &str,
    expr: &Expr,
    sites: &mut std::collections::HashMap<usize, ParamSite>,
) {
    if let Some(n) = placeholder_index(expr) {
        let site = if column.eq_ignore_ascii_case("id") {
            ParamSite::IdColumn
        } else {
            ParamSite::Column(column.to_string())
        };
        sites.entry(n).or_insert(site);
    }
}

/// Walk a SELECT's WHERE/HAVING for `col OP $n` param sites (best-effort).
fn collect_query_param_sites(
    q: &datafusion::sql::sqlparser::ast::Query,
    sites: &mut std::collections::HashMap<usize, ParamSite>,
) {
    if let SetExpr::Select(select) = q.body.as_ref() {
        if let Some(sel) = &select.selection {
            collect_expr_param_sites(sel, sites);
        }
    }
}

/// Walk an expression tree, recording `<col> OP $n` (or `$n OP <col>`) sites and a
/// `$n OP <literal>` literal-typed site. Recurses through AND/OR/binary ops.
fn collect_expr_param_sites(expr: &Expr, sites: &mut std::collections::HashMap<usize, ParamSite>) {
    if let Expr::BinaryOp { left, op: _, right } = expr {
        // A param on one side typed from the OTHER side.
        if let Some(n) = placeholder_index(right) {
            sites.entry(n).or_insert_with(|| site_from_operand(left));
        }
        if let Some(n) = placeholder_index(left) {
            sites.entry(n).or_insert_with(|| site_from_operand(right));
        }
        collect_expr_param_sites(left, sites);
        collect_expr_param_sites(right, sites);
    } else if let Expr::Nested(inner) = expr {
        collect_expr_param_sites(inner, sites);
    }
}

/// Type a param from the operand it sits opposite in a comparison: a column → the
/// column site; a literal → its literal type; anything else → Text.
fn site_from_operand(operand: &Expr) -> ParamSite {
    match operand {
        Expr::Identifier(id) => {
            if id.value.eq_ignore_ascii_case("id") {
                ParamSite::IdColumn
            } else {
                ParamSite::Column(id.value.clone())
            }
        }
        Expr::CompoundIdentifier(parts) => match parts.last() {
            Some(i) if i.value.eq_ignore_ascii_case("id") => ParamSite::IdColumn,
            Some(i) => ParamSite::Column(i.value.clone()),
            None => ParamSite::Literal(ParamLiteralType::Text),
        },
        other => match expr_to_json(other) {
            Ok(Value::Number(n)) if n.is_i64() || n.is_u64() => {
                ParamSite::Literal(ParamLiteralType::Int)
            }
            Ok(Value::Number(_)) => ParamSite::Literal(ParamLiteralType::Float),
            Ok(Value::Bool(_)) => ParamSite::Literal(ParamLiteralType::Bool),
            _ => ParamSite::Literal(ParamLiteralType::Text),
        },
    }
}

/// The 1-based index of a `$N` placeholder expression, if `expr` is exactly one.
fn placeholder_index(expr: &Expr) -> Option<usize> {
    if let Expr::Value(SqlValue::Placeholder(p)) = expr {
        p.strip_prefix('$').and_then(|d| d.parse::<usize>().ok())
    } else {
        None
    }
}

/// Rewrite a READ statement into a SCHEMA-PROBE form for the extended-protocol
/// Describe step (CONCEPT:KG-2.197): drop the `WHERE`/`HAVING` predicate and any
/// `LIMIT`/`OFFSET` so the probe returns ROWS regardless of the (unbound) parameter
/// values. The projection, `FROM`, joins, and `GROUP BY` are KEPT, so the result
/// COLUMN schema is identical to the real query — but the engine's schema-on-read
/// path (which can drop the column schema when a filtered query yields ZERO rows)
/// always sees rows, so the described columns are stable. Returns `None` for a
/// non-SELECT or a shape we can't safely rewrite (the caller then falls back to
/// running the substituted SQL as-is).
pub fn schema_probe_sql(sql: &str) -> Option<String> {
    let mut stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    let stmt = match stmts.as_mut_slice() {
        [s] => s,
        _ => return None,
    };
    let Statement::Query(query) = stmt else {
        return None;
    };
    // Drop row-limiting clauses — they don't change the column schema.
    query.limit = None;
    query.offset = None;
    // Neutralize the predicate(s) of the (possibly nested) SELECT body so all rows
    // pass and the schema is stable.
    neutralize_select_predicates(&mut query.body);
    Some(query.to_string())
}

/// Replace a SELECT's `WHERE`/`HAVING` with `TRUE` (recursing through set
/// operations) so a schema-probe query returns rows. Leaves projection/FROM/GROUP BY
/// intact.
fn neutralize_select_predicates(body: &mut SetExpr) {
    match body {
        SetExpr::Select(select) => {
            if select.selection.is_some() {
                select.selection = Some(true_expr());
            }
            if select.having.is_some() {
                select.having = Some(true_expr());
            }
        }
        SetExpr::Query(q) => neutralize_select_predicates(&mut q.body),
        SetExpr::SetOperation { left, right, .. } => {
            neutralize_select_predicates(left);
            neutralize_select_predicates(right);
        }
        _ => {}
    }
}

/// The literal `TRUE` expression.
fn true_expr() -> Expr {
    Expr::Value(SqlValue::Boolean(true))
}

/// The explicit RETURNING projection column names of a write, if the statement has
/// a `RETURNING <col>[, …]` with named columns (not `*`). Lets the extended-protocol
/// Describe report a RETURNING write's result columns WITHOUT executing the write.
/// `None` ⇒ not a write / no RETURNING / `RETURNING *` (caller falls back).
pub fn returning_columns(sql: &str) -> Option<Vec<String>> {
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    let stmt = match stmts.as_slice() {
        [s] => s,
        _ => return None,
    };
    let items = match stmt {
        Statement::Insert(insert) => insert.returning.as_ref(),
        Statement::Update { returning, .. } => returning.as_ref(),
        Statement::Delete(delete) => delete.returning.as_ref(),
        _ => None,
    }?;
    let mut cols = Vec::new();
    for it in items {
        match it {
            SelectItem::UnnamedExpr(Expr::Identifier(id)) => cols.push(id.value.clone()),
            SelectItem::ExprWithAlias { alias, .. } => cols.push(alias.value.clone()),
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                cols.push(parts.last()?.value.clone())
            }
            // `RETURNING *` or an expression projection → can't name statically.
            _ => return None,
        }
    }
    Some(cols)
}

/// Rewrite the pgvector distance operators (CONCEPT:EG-115) AND the ParadeDB BM25
/// operators/functions (CONCEPT:EG-119) in `sql` to the engine's registered scalar UDF
/// calls, so DataFusion — which has no operator for them — can plan the query:
///   * `a <-> b` (L2 distance)              → `vector_l2(a, b)`
///   * `a <=> b` (cosine distance)          → `vector_cosine(a, b)`
///   * `a <#> b` (negative inner product)   → `vector_ip(a, b)`
///   * `col @@@ 'q'` (BM25 match, EG-119)   → `bm25_match(col, 'q')`
///   * `paradedb.score(x)` / `.snippet(x)`  → `bm25_score(x)` / `bm25_snippet(x)`
///
/// Pure text→AST→text: parse with the SAME Postgres dialect, walk the query's
/// projection / WHERE / HAVING / ORDER BY expressions replacing the operators, then
/// re-serialize. Returns the ORIGINAL `sql` unchanged when it doesn't parse or contains
/// no vector operator (so a query that DataFusion parses but `sqlparser` doesn't is
/// never perturbed). The `ORDER BY emb <-> '[1,2,3]' LIMIT k` nearest-neighbour shape is
/// the primary target; the eg-ann INDEX pushdown is a separate later item (EG-116).
pub fn desugar_vector_ops(sql: &str) -> String {
    let Ok(mut stmts) = Parser::parse_sql(&PostgreSqlDialect {}, sql) else {
        return sql.to_string();
    };
    let mut changed = false;
    for stmt in &mut stmts {
        if let Statement::Query(q) = stmt {
            rewrite_query_vector_ops(q, &mut changed);
        }
    }
    if changed {
        stmts
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join("; ")
    } else {
        sql.to_string()
    }
}

/// Rewrite vector operators throughout a `Query` (body clauses + ORDER BY).
fn rewrite_query_vector_ops(q: &mut datafusion::sql::sqlparser::ast::Query, changed: &mut bool) {
    rewrite_setexpr_vector_ops(&mut q.body, changed);
    if let Some(order_by) = &mut q.order_by {
        for ob in &mut order_by.exprs {
            rewrite_expr_vector_ops(&mut ob.expr, changed);
        }
    }
}

/// Rewrite vector operators in a `SetExpr` (a SELECT body or a UNION/INTERSECT arm).
fn rewrite_setexpr_vector_ops(body: &mut SetExpr, changed: &mut bool) {
    match body {
        SetExpr::Select(select) => {
            for item in &mut select.projection {
                match item {
                    SelectItem::UnnamedExpr(e) => rewrite_expr_vector_ops(e, changed),
                    SelectItem::ExprWithAlias { expr, .. } => {
                        rewrite_expr_vector_ops(expr, changed)
                    }
                    _ => {}
                }
            }
            if let Some(sel) = &mut select.selection {
                rewrite_expr_vector_ops(sel, changed);
            }
            if let Some(having) = &mut select.having {
                rewrite_expr_vector_ops(having, changed);
            }
        }
        SetExpr::Query(q) => rewrite_query_vector_ops(q, changed),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_setexpr_vector_ops(left, changed);
            rewrite_setexpr_vector_ops(right, changed);
        }
        _ => {}
    }
}

/// Recursively rewrite vector operators in an expression tree. Recurses into operands
/// FIRST (so a nested `a <-> (b <=> c)` fully desugars), then replaces a top-level
/// vector-operator `BinaryOp` with the corresponding UDF call. The call node is built
/// by re-parsing `fname(left, right)` (both operands already serialize to valid SQL),
/// avoiding a version-fragile hand-construction of `sqlparser`'s `Function` AST.
fn rewrite_expr_vector_ops(expr: &mut Expr, changed: &mut bool) {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            rewrite_expr_vector_ops(left, changed);
            rewrite_expr_vector_ops(right, changed);
            // CONCEPT:EG-119 — ParadeDB `col @@@ 'query'` BM25 search. `@@@` tokenizes as
            // `AtAt` with a `@`(PGAbs)-wrapped right operand; rewrite to `bm25_match(col,
            // 'query')` so DataFusion can plan a lexical filter (the eg-text BM25 pushdown
            // + ranking is the server-side lowering).
            if matches!(op, BinaryOperator::AtAt) {
                if let Expr::UnaryOp {
                    op: UnaryOperator::PGAbs,
                    expr: inner,
                } = right.as_ref()
                {
                    let rhs = (**inner).clone();
                    if let Some(call) = vector_udf_call("bm25_match", left, &rhs) {
                        *expr = call;
                        *changed = true;
                        return;
                    }
                }
            }
            let fname = match op {
                BinaryOperator::Custom(s) if s == "<->" => Some("vector_l2"),
                BinaryOperator::Custom(s) if s == "<#>" => Some("vector_ip"),
                // `<=>` parses as `Spaceship`; in a pgvector context it is cosine distance.
                BinaryOperator::Spaceship => Some("vector_cosine"),
                _ => None,
            };
            if let Some(fname) = fname {
                if let Some(call) = vector_udf_call(fname, left, right) {
                    *expr = call;
                    *changed = true;
                }
            }
        }
        // CONCEPT:EG-119 — `paradedb.score(x)`/`paradedb.snippet(x)` → the registered
        // `bm25_score`/`bm25_snippet` UDFs. Rename the function + recurse into its args.
        Expr::Function(f) => {
            if f.name.0.len() == 2 && f.name.0[0].value.eq_ignore_ascii_case("paradedb") {
                let renamed = match f.name.0[1].value.to_ascii_lowercase().as_str() {
                    "score" => Some("bm25_score"),
                    "snippet" => Some("bm25_snippet"),
                    _ => None,
                };
                if let Some(new_name) = renamed {
                    f.name.0 = vec![datafusion::sql::sqlparser::ast::Ident::new(new_name)];
                    *changed = true;
                }
            }
            if let FunctionArguments::List(list) = &mut f.args {
                for arg in &mut list.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(e),
                        ..
                    } = arg
                    {
                        rewrite_expr_vector_ops(e, changed);
                    }
                }
            }
        }
        Expr::Nested(inner) => rewrite_expr_vector_ops(inner, changed),
        Expr::UnaryOp { expr, .. } => rewrite_expr_vector_ops(expr, changed),
        Expr::Cast { expr, .. } => rewrite_expr_vector_ops(expr, changed),
        _ => {}
    }
}

/// Build the `fname(left, right)` call expression by re-parsing its SQL text (both
/// operands already serialize to valid SQL). `None` if the (internally-generated) text
/// fails to parse — the caller then leaves the operator in place.
fn vector_udf_call(fname: &str, left: &Expr, right: &Expr) -> Option<Expr> {
    let text = format!("{fname}({left}, {right})");
    Parser::new(&PostgreSqlDialect {})
        .try_with_sql(&text)
        .ok()?
        .parse_expr()
        .ok()
}

/// Decode `INSERT INTO nodes (id, …) VALUES (…)[, (…)…]` into [`InsertNodes`].
/// Only the `nodes` table, a column list including `id`, and literal `VALUES`
/// rows are accepted — anything else is an explicit error (no silent mis-route).
/// Multiple `VALUES` rows produce multiple [`InsertNode`]s (CONCEPT:KG-2.198).
fn classify_insert(insert: &Insert) -> Result<InsertNodes, String> {
    require_nodes_table(&insert.table_name.to_string(), "INSERT")?;
    if insert.columns.is_empty() {
        return Err(
            "INSERT INTO nodes requires an explicit column list including `id`".to_string(),
        );
    }
    let columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();

    let source = insert
        .source
        .as_ref()
        .ok_or("INSERT INTO nodes requires a VALUES clause")?;
    let value_rows = match source.body.as_ref() {
        SetExpr::Values(Values { rows, .. }) => rows,
        _ => return Err("INSERT INTO nodes supports only literal VALUES rows".to_string()),
    };
    if value_rows.is_empty() {
        return Err("INSERT INTO nodes VALUES has no rows".to_string());
    }

    let mut rows = Vec::with_capacity(value_rows.len());
    for row in value_rows {
        rows.push(decode_insert_row(&columns, row)?);
    }
    Ok(InsertNodes {
        rows,
        returning: insert.returning.is_some(),
        on_conflict: decode_on_conflict(insert.on.as_ref())?,
    })
}

/// Decode a sqlparser `ON CONFLICT` clause into an [`OnConflict`] (CONCEPT:EG-048).
/// `None`/absent ⇒ no upsert. A MySQL `ON DUPLICATE KEY UPDATE` and an
/// `ON CONFLICT … DO UPDATE` with a `WHERE` are rejected (explicit follow-ups).
fn decode_on_conflict(on: Option<&OnInsert>) -> Result<Option<OnConflict>, String> {
    let Some(on) = on else {
        return Ok(None);
    };
    let conflict = match on {
        OnInsert::OnConflict(c) => c,
        OnInsert::DuplicateKeyUpdate(_) => {
            return Err("ON DUPLICATE KEY UPDATE is not supported; use ON CONFLICT".to_string())
        }
        _ => return Err("unsupported INSERT conflict clause".to_string()),
    };
    let target_cols = match &conflict.conflict_target {
        Some(ConflictTarget::Columns(cols)) => cols.iter().map(|c| c.value.clone()).collect(),
        Some(ConflictTarget::OnConstraint(_)) => {
            return Err("ON CONFLICT ON CONSTRAINT is not supported (name the columns)".to_string())
        }
        None => Vec::new(),
    };
    let action = match &conflict.action {
        SqlOnConflictAction::DoNothing => OnConflictAction::DoNothing,
        SqlOnConflictAction::DoUpdate(do_update) => {
            if do_update.selection.is_some() {
                return Err(
                    "ON CONFLICT DO UPDATE … WHERE is not supported (CONCEPT:EG-048)".to_string(),
                );
            }
            let mut set = Map::new();
            for a in &do_update.assignments {
                let col = match &a.target {
                    AssignmentTarget::ColumnName(name) => last_ident(name),
                    AssignmentTarget::Tuple(_) => {
                        return Err("ON CONFLICT DO UPDATE tuple assignment is not supported"
                            .to_string())
                    }
                };
                if col.eq_ignore_ascii_case("id") {
                    return Err("ON CONFLICT DO UPDATE cannot reassign the `id` column".to_string());
                }
                set.insert(col, expr_to_json(&a.value)?);
            }
            OnConflictAction::DoUpdate(set)
        }
    };
    Ok(Some(OnConflict {
        target_cols,
        action,
    }))
}

/// Decode one `VALUES (…)` row against the column list into an [`InsertNode`].
fn decode_insert_row(columns: &[String], row: &[Expr]) -> Result<InsertNode, String> {
    if row.len() != columns.len() {
        return Err(format!(
            "INSERT column/value count mismatch: {} columns, {} values",
            columns.len(),
            row.len()
        ));
    }
    let mut node_id: Option<String> = None;
    let mut properties = Map::new();
    for (col, expr) in columns.iter().zip(row.iter()) {
        let val = expr_to_json(expr)?;
        if col.eq_ignore_ascii_case("id") {
            node_id = Some(scalar_id(val)?);
        } else {
            properties.insert(col.clone(), val);
        }
    }
    let node_id = node_id.ok_or("INSERT INTO nodes must set the `id` column")?;
    Ok(InsertNode {
        node_id,
        properties,
    })
}

/// Decode `UPDATE nodes SET k = v[, …] WHERE <simple eq>` into [`UpdateNodes`].
fn classify_update(
    table: &datafusion::sql::sqlparser::ast::TableWithJoins,
    assignments: &[Assignment],
    from: Option<&datafusion::sql::sqlparser::ast::TableWithJoins>,
    selection: Option<&Expr>,
    returning: &Option<Vec<SelectItem>>,
) -> Result<UpdateNodes, String> {
    require_nodes_target(table, "UPDATE")?;
    // CONCEPT:EG-047 — a `FROM` clause is routed to `classify_update_nodes_join` by the
    // caller before reaching here, so `from` is always `None` on this simple path.
    let _ = from;
    if assignments.is_empty() {
        return Err("UPDATE nodes requires at least one SET assignment".to_string());
    }
    let mut set = Map::new();
    for a in assignments {
        let col = match &a.target {
            AssignmentTarget::ColumnName(name) => last_ident(name),
            AssignmentTarget::Tuple(_) => {
                return Err("UPDATE nodes tuple assignment is not supported".to_string())
            }
        };
        if col.eq_ignore_ascii_case("id") {
            return Err("UPDATE nodes cannot reassign the `id` column".to_string());
        }
        set.insert(col, expr_to_json(&a.value)?);
    }
    let selector = decode_where(selection, "UPDATE")?;
    Ok(UpdateNodes {
        set,
        selector,
        returning: returning.is_some(),
    })
}

/// Decode `DELETE FROM nodes WHERE <simple eq>` into [`DeleteNodes`].
fn classify_delete(delete: &Delete) -> Result<DeleteNodes, String> {
    if delete.using.is_some() {
        return Err("DELETE … USING is not supported (CONCEPT:KG-2.198 follow-up)".to_string());
    }
    let tables = match &delete.from {
        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
    };
    let target = match tables.as_slice() {
        [one] => one,
        _ => return Err("DELETE FROM supports exactly one table (`nodes`)".to_string()),
    };
    require_nodes_target(target, "DELETE")?;
    let selector = decode_where(delete.selection.as_ref(), "DELETE")?;
    Ok(DeleteNodes {
        selector,
        returning: delete.returning.is_some(),
    })
}

// ── user-table dispatch + DDL/DML parsing (CONCEPT:EG-018) ───────────────────

/// `nodes`/`edges` are the graph projection's reserved table names — a user
/// `CREATE TABLE`/DML cannot use them, and DML routing sends them to the graph path.
fn is_reserved_table(leaf: &str) -> bool {
    leaf.eq_ignore_ascii_case("nodes") || leaf.eq_ignore_ascii_case("edges")
}

/// The bare (last-segment) name of an INSERT target.
fn insert_leaf(insert: &Insert) -> String {
    let s = insert.table_name.to_string();
    s.rsplit('.').next().unwrap_or(&s).to_string()
}

/// Route an INSERT by target table: `nodes` → the graph node path (unchanged);
/// `edges` → rejected (graph edge writes are not a wire DML shape); any other table
/// → the user-table path (literal VALUES or `INSERT … SELECT`).
fn classify_any_insert(insert: &Insert) -> Result<StatementKind, String> {
    let leaf = insert_leaf(insert);
    if leaf.eq_ignore_ascii_case("nodes") {
        // A SELECT/WITH body → `INSERT INTO nodes … SELECT` (CONCEPT:EG-046); a literal
        // VALUES body → the existing single/multi-row node insert.
        let is_select = matches!(
            insert.source.as_ref().map(|s| s.body.as_ref()),
            Some(SetExpr::Select(_)) | Some(SetExpr::Query(_)) | Some(SetExpr::SetOperation { .. })
        );
        if is_select {
            return classify_insert_nodes_select(insert);
        }
        return classify_insert(insert).map(StatementKind::InsertNodes);
    }
    if leaf.eq_ignore_ascii_case("edges") {
        return Err("INSERT is only supported on the `nodes` table".to_string());
    }
    classify_insert_table(insert, leaf)
}

/// Decode `INSERT INTO nodes (id, …) SELECT …` (CONCEPT:EG-046). The column list must
/// include `id`; the SELECT body is kept as text to re-run through the DataFusion read
/// path (so it may JOIN user tables and the graph).
fn classify_insert_nodes_select(insert: &Insert) -> Result<StatementKind, String> {
    if insert.columns.is_empty() {
        return Err(
            "INSERT INTO nodes … SELECT requires an explicit column list including `id`"
                .to_string(),
        );
    }
    let columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();
    if !columns.iter().any(|c| c.eq_ignore_ascii_case("id")) {
        return Err("INSERT INTO nodes … SELECT column list must include `id`".to_string());
    }
    let source = insert
        .source
        .as_ref()
        .ok_or("INSERT INTO nodes … SELECT requires a SELECT source")?;
    Ok(StatementKind::InsertNodesSelect(InsertNodesSelect {
        columns,
        select_sql: source.to_string(),
        returning: insert.returning.is_some(),
        on_conflict: decode_on_conflict(insert.on.as_ref())?,
    }))
}

/// Decode an `INSERT INTO <user_table> …` into either a literal [`InsertTable`] (a
/// `VALUES` body) or an [`InsertSelect`] (a `SELECT`/`WITH` body run through DataFusion).
fn classify_insert_table(insert: &Insert, table: String) -> Result<StatementKind, String> {
    if insert.columns.is_empty() {
        return Err(format!(
            "INSERT INTO {table} requires an explicit column list"
        ));
    }
    let columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();
    let source = insert
        .source
        .as_ref()
        .ok_or_else(|| format!("INSERT INTO {table} requires a VALUES or SELECT source"))?;
    match source.body.as_ref() {
        SetExpr::Values(Values { rows, .. }) => {
            if rows.is_empty() {
                return Err(format!("INSERT INTO {table} VALUES has no rows"));
            }
            let mut out_rows = Vec::with_capacity(rows.len());
            for row in rows {
                if row.len() != columns.len() {
                    return Err(format!(
                        "INSERT column/value count mismatch: {} columns, {} values",
                        columns.len(),
                        row.len()
                    ));
                }
                let mut vals = Vec::with_capacity(row.len());
                for expr in row {
                    vals.push(expr_to_json(expr)?);
                }
                out_rows.push(vals);
            }
            Ok(StatementKind::InsertTable(InsertTable {
                table,
                columns,
                rows: out_rows,
                on_conflict: decode_on_conflict(insert.on.as_ref())?,
                returning: insert.returning.is_some(),
            }))
        }
        // A SELECT/WITH/UNION source → INSERT … SELECT. Keep the source query as text;
        // the executor runs it through the SAME DataFusion path (graph + user tables).
        _ => Ok(StatementKind::InsertSelect(InsertSelect {
            table,
            columns,
            select_sql: source.to_string(),
        })),
    }
}

/// Route an UPDATE by target table (mirrors [`classify_any_insert`]).
fn classify_any_update(
    table: &TableWithJoins,
    assignments: &[Assignment],
    from: Option<&TableWithJoins>,
    selection: Option<&Expr>,
    returning: &Option<Vec<SelectItem>>,
) -> Result<StatementKind, String> {
    let leaf = match &table.relation {
        TableFactor::Table { name, .. } => {
            let s = name.to_string();
            s.rsplit('.').next().unwrap_or(&s).to_string()
        }
        _ => return Err("UPDATE target must be a table".to_string()),
    };
    if leaf.eq_ignore_ascii_case("nodes") {
        // CONCEPT:EG-047 — a `FROM <other>` clause or a JOIN on the target makes this a
        // correlated multi-table update; resolve ids + per-row SET values via DataFusion.
        if from.is_some() || !table.joins.is_empty() {
            return classify_update_nodes_join(table, assignments, from, selection, returning)
                .map(StatementKind::UpdateNodesJoin);
        }
        return classify_update(table, assignments, from, selection, returning)
            .map(StatementKind::UpdateNodes);
    }
    if leaf.eq_ignore_ascii_case("edges") {
        return Err("UPDATE is only supported on the `nodes` table".to_string());
    }
    classify_update_table(leaf, table, assignments, from, selection).map(StatementKind::UpdateTable)
}

/// Decode `UPDATE <user_table> SET … WHERE <col> = <literal>`.
fn classify_update_table(
    table: String,
    target: &TableWithJoins,
    assignments: &[Assignment],
    from: Option<&TableWithJoins>,
    selection: Option<&Expr>,
) -> Result<UpdateTable, String> {
    if !target.joins.is_empty() {
        return Err("UPDATE with a JOIN is not supported (EG-018 follow-up)".to_string());
    }
    if from.is_some() {
        return Err("UPDATE … FROM is not supported (EG-018 follow-up)".to_string());
    }
    if assignments.is_empty() {
        return Err(format!(
            "UPDATE {table} requires at least one SET assignment"
        ));
    }
    let mut set = Map::new();
    for a in assignments {
        let col = match &a.target {
            AssignmentTarget::ColumnName(name) => last_ident(name),
            AssignmentTarget::Tuple(_) => {
                return Err("UPDATE tuple assignment is not supported".to_string())
            }
        };
        set.insert(col, expr_to_json(&a.value)?);
    }
    let selector = decode_table_where(selection, "UPDATE", &table)?;
    Ok(UpdateTable {
        table,
        set,
        selector,
    })
}

/// Route a DELETE by target table (mirrors [`classify_any_insert`]).
fn classify_any_delete(delete: &Delete) -> Result<StatementKind, String> {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
    };
    let target = match tables.as_slice() {
        [one] => one,
        _ => return Err("DELETE FROM supports exactly one table".to_string()),
    };
    let leaf = match &target.relation {
        TableFactor::Table { name, .. } => {
            let s = name.to_string();
            s.rsplit('.').next().unwrap_or(&s).to_string()
        }
        _ => return Err("DELETE target must be a table".to_string()),
    };
    if leaf.eq_ignore_ascii_case("nodes") {
        // CONCEPT:EG-047 — a `USING <other>` clause or a JOIN on the target makes this a
        // correlated multi-table delete; resolve ids via DataFusion.
        if delete.using.is_some() || !target.joins.is_empty() {
            return classify_delete_nodes_join(
                target,
                delete.using.as_ref(),
                delete.selection.as_ref(),
                &delete.returning,
            )
            .map(StatementKind::DeleteNodesJoin);
        }
        return classify_delete(delete).map(StatementKind::DeleteNodes);
    }
    if leaf.eq_ignore_ascii_case("edges") {
        return Err("DELETE is only supported on the `nodes` table".to_string());
    }
    if delete.using.is_some() || !target.joins.is_empty() {
        return Err("DELETE … USING/JOIN is not supported for user tables".to_string());
    }
    let selector = decode_table_where(delete.selection.as_ref(), "DELETE", &leaf)?;
    Ok(StatementKind::DeleteTable(DeleteTable {
        table: leaf,
        selector,
    }))
}

/// Decode a user-table WHERE clause into a single `<col> = <literal>` predicate (no
/// `id` special-casing). A missing WHERE is rejected (no unscoped mass mutation).
fn decode_table_where(
    selection: Option<&Expr>,
    verb: &str,
    table: &str,
) -> Result<TableWhereEq, String> {
    let expr = selection.ok_or_else(|| {
        format!("{verb} {table} requires a WHERE clause (unscoped {verb} refused)")
    })?;
    // CONCEPT:EG-045 — decode the full compound predicate; the store evaluates it
    // per row inside its write transaction.
    let pred = decode_predicate(expr)?;
    Ok(TableWhereEq { pred })
}

/// Decode `CREATE TABLE name (col type [NOT NULL] [PRIMARY KEY], …) [IF NOT EXISTS]`.
fn classify_create_table(ct: &CreateTable) -> Result<CreateTablePlan, String> {
    let name = last_ident(&ct.name);
    if is_reserved_table(&name) {
        return Err(format!(
            "CREATE TABLE cannot use the reserved graph table name `{name}`"
        ));
    }
    if ct.columns.is_empty() {
        return Err("CREATE TABLE requires at least one column".to_string());
    }
    let mut columns = Vec::with_capacity(ct.columns.len());
    for c in &ct.columns {
        columns.push(decode_column_def(c)?);
    }
    Ok(CreateTablePlan {
        name,
        columns,
        if_not_exists: ct.if_not_exists,
    })
}

/// Decode one sqlparser column definition into a [`ColumnDef`] (CONCEPT:EG-018 +
/// constraints CONCEPT:EG-020): name, raw type spelling, NULL/NOT NULL/PRIMARY KEY,
/// UNIQUE, column DEFAULT (literal or `nextval` ⇒ SERIAL), SERIAL/BIGSERIAL types, and
/// a simple `CHECK (col OP literal)`. A `PRIMARY KEY` column is implicitly NOT NULL.
fn decode_column_def(c: &SqlColumnDef) -> Result<ColumnDef, String> {
    let type_name = c.data_type.to_string();
    // SERIAL/BIGSERIAL: auto-increment + NOT NULL (Postgres pseudo-types).
    let base_ty = type_name
        .split('(')
        .next()
        .unwrap_or(&type_name)
        .trim()
        .to_ascii_lowercase();
    let mut serial = matches!(
        base_ty.as_str(),
        "serial" | "bigserial" | "smallserial" | "serial4" | "serial8"
    );
    let mut nullable = !serial; // SERIAL ⇒ NOT NULL by default
    let mut primary_key = false;
    let mut unique = false;
    let mut default = None;
    let mut check = None;
    for opt in &c.options {
        match &opt.option {
            ColumnOption::NotNull => nullable = false,
            ColumnOption::Null => nullable = true,
            ColumnOption::Unique { is_primary, .. } => {
                unique = true;
                if *is_primary {
                    primary_key = true;
                    nullable = false;
                }
            }
            ColumnOption::Default(expr) => {
                // `DEFAULT nextval('…')` ⇒ a sequence (SERIAL); a literal ⇒ a default value.
                if is_nextval(expr) {
                    serial = true;
                    nullable = false;
                } else {
                    default = Some(expr_to_json(expr).map_err(|e| {
                        format!(
                            "DEFAULT on column `{}` must be a literal: {e}",
                            c.name.value
                        )
                    })?);
                }
            }
            ColumnOption::Check(expr) => {
                check = Some(decode_check(expr, &c.name.value)?);
            }
            _ => {}
        }
    }
    Ok(ColumnDef {
        name: c.name.value.clone(),
        type_name,
        nullable,
        primary_key,
        unique,
        serial,
        default,
        check,
    })
}

/// Whether `expr` is a `nextval(...)` call (the `DEFAULT nextval('seq')` SERIAL idiom).
fn is_nextval(expr: &Expr) -> bool {
    if let Expr::Function(f) = expr {
        return last_ident(&f.name).eq_ignore_ascii_case("nextval");
    }
    false
}

/// Decode a simple `CHECK (col OP literal)` into a [`ColCheck`] (CONCEPT:EG-020). The
/// left side must be a column (its name is not re-checked — the constraint is enforced
/// on the column it is declared on); the right must be a literal. A complex CHECK
/// (AND/OR, functions, cross-column) is rejected so it is never silently dropped.
fn decode_check(expr: &Expr, col: &str) -> Result<ColCheck, String> {
    let inner = match expr {
        Expr::Nested(e) => e.as_ref(),
        other => other,
    };
    if let Expr::BinaryOp { left, op, right } = inner {
        let op = match op {
            BinaryOperator::Eq => CmpOp::Eq,
            BinaryOperator::NotEq => CmpOp::Ne,
            BinaryOperator::Lt => CmpOp::Lt,
            BinaryOperator::LtEq => CmpOp::Le,
            BinaryOperator::Gt => CmpOp::Gt,
            BinaryOperator::GtEq => CmpOp::Ge,
            other => {
                return Err(format!(
                    "CHECK on `{col}` supports only a simple comparison, got operator `{other}`"
                ))
            }
        };
        // Accept `col OP literal` or `literal OP col`.
        let (value, flip) = if matches!(
            left.as_ref(),
            Expr::Identifier(_) | Expr::CompoundIdentifier(_)
        ) {
            (expr_to_json(right)?, false)
        } else if matches!(
            right.as_ref(),
            Expr::Identifier(_) | Expr::CompoundIdentifier(_)
        ) {
            (expr_to_json(left)?, true)
        } else {
            return Err(format!(
                "CHECK on `{col}` must compare the column to a literal"
            ));
        };
        let op = if flip { flip_cmp(op) } else { op };
        return Ok(ColCheck { op, value });
    }
    Err(format!(
        "CHECK on `{col}` supports only a simple `{col} OP literal` predicate"
    ))
}

/// Mirror a comparison operator when the column is on the RIGHT (`5 < col` ⇒ `col > 5`).
fn flip_cmp(op: CmpOp) -> CmpOp {
    match op {
        CmpOp::Lt => CmpOp::Gt,
        CmpOp::Le => CmpOp::Ge,
        CmpOp::Gt => CmpOp::Lt,
        CmpOp::Ge => CmpOp::Le,
        other => other,
    }
}

/// Decode `DROP TABLE [IF EXISTS] name` or `DROP VIEW [IF EXISTS] name`
/// (CONCEPT:EG-072). Only `TABLE`/`VIEW` objects are handled; any other `DROP …` is
/// rejected so it isn't silently mis-routed.
fn classify_drop(
    object_type: ObjectType,
    if_exists: bool,
    names: &[ObjectName],
) -> Result<StatementKind, String> {
    let name = match names {
        [one] => last_ident(one),
        _ => return Err("DROP supports exactly one object".to_string()),
    };
    match object_type {
        ObjectType::Table => {
            if is_reserved_table(&name) {
                return Err(format!("cannot DROP the reserved graph table `{name}`"));
            }
            Ok(StatementKind::DropTable(DropTablePlan { name, if_exists }))
        }
        ObjectType::View => Ok(StatementKind::DropView(DropViewPlan { name, if_exists })),
        other => Err(format!("DROP {other} is not supported (only TABLE/VIEW)")),
    }
}

/// Decode `CREATE [OR REPLACE] VIEW name AS <select>` (CONCEPT:EG-072). Read-only:
/// the view name may not shadow a reserved graph table, the body must be a plain
/// query, and MATERIALIZED views are rejected (a follow-up).
fn classify_create_view(
    name: &ObjectName,
    query: &datafusion::sql::sqlparser::ast::Query,
    or_replace: bool,
    materialized: bool,
) -> Result<StatementKind, String> {
    if materialized {
        return Err("CREATE MATERIALIZED VIEW is not supported (CONCEPT:EG-072)".to_string());
    }
    let name = last_ident(name);
    if is_reserved_table(&name) {
        return Err(format!(
            "CREATE VIEW cannot use the reserved graph table name `{name}`"
        ));
    }
    Ok(StatementKind::CreateView(CreateViewPlan {
        name,
        select_sql: query.to_string(),
        or_replace,
    }))
}

/// The extension names the engine recognizes (CONCEPT:EG-102). `CREATE EXTENSION` on
/// one of these is accepted + recorded so a client's setup script proceeds; each
/// extension's concrete surface (pgvector types/ops EG-115, AGE, TimescaleDB,
/// pg_search) lands in its own later item.
fn is_recognized_extension(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "vector" | "pg_age" | "age" | "timescaledb" | "pg_search"
    )
}

/// Decode `CREATE EXTENSION [IF NOT EXISTS] name [WITH SCHEMA …]` (CONCEPT:EG-102).
/// A recognized extension is accepted + recorded; an unknown name is rejected with a
/// precise error (never silently accepted) so a client learns the surface is absent.
fn classify_create_extension(name: &str, if_not_exists: bool) -> Result<StatementKind, String> {
    if !is_recognized_extension(name) {
        return Err(format!(
            "CREATE EXTENSION `{name}` is not recognized (supported: \
             vector, pg_age/age, timescaledb, pg_search)"
        ));
    }
    Ok(StatementKind::CreateExtension {
        name: name.to_string(),
        if_not_exists,
    })
}

/// Recognize `DROP EXTENSION [IF EXISTS] name [CASCADE|RESTRICT]` textually
/// (CONCEPT:EG-102) — `sqlparser` 0.51 has no `DROP EXTENSION` AST node. Returns
/// `(name, if_exists)` when the statement is a single-extension drop, else `None`
/// (so `classify` falls through to the parser for every non-`DROP EXTENSION` input).
fn parse_drop_extension(sql: &str) -> Option<(String, bool)> {
    let trimmed = sql.trim().trim_end_matches(';');
    let mut toks = trimmed.split_whitespace();
    if !toks.next()?.eq_ignore_ascii_case("DROP") {
        return None;
    }
    if !toks.next()?.eq_ignore_ascii_case("EXTENSION") {
        return None;
    }
    // Optional `IF EXISTS`.
    let mut peek = toks.next()?;
    let mut if_exists = false;
    if peek.eq_ignore_ascii_case("IF") {
        if !toks.next()?.eq_ignore_ascii_case("EXISTS") {
            return None;
        }
        if_exists = true;
        peek = toks.next()?;
    }
    // A single extension name (a comma-list or trailing junk beyond CASCADE/RESTRICT
    // is not this simple shape).
    let name = peek.trim_matches('"');
    if name.is_empty() || name.contains(',') {
        return None;
    }
    // Only CASCADE/RESTRICT may follow.
    if let Some(tail) = toks.next() {
        if !tail.eq_ignore_ascii_case("CASCADE") && !tail.eq_ignore_ascii_case("RESTRICT") {
            return None;
        }
    }
    Some((name.to_string(), if_exists))
}

// ── SQL stored functions (CONCEPT:EG-118) ─────────────────────────────────────

/// Whether `sql` begins `CREATE [OR REPLACE] FUNCTION …` (case-insensitive). Used to
/// route to the textual [`parse_create_function`] before the parser (CONCEPT:EG-118).
fn is_create_function(sql: &str) -> bool {
    let mut toks = sql.trim_start().split_whitespace();
    if !toks.next().is_some_and(|t| t.eq_ignore_ascii_case("CREATE")) {
        return false;
    }
    let mut next = match toks.next() {
        Some(t) => t,
        None => return false,
    };
    if next.eq_ignore_ascii_case("OR") {
        if !toks.next().is_some_and(|t| t.eq_ignore_ascii_case("REPLACE")) {
            return false;
        }
        next = match toks.next() {
            Some(t) => t,
            None => return false,
        };
    }
    // The FUNCTION keyword may be glued to the name's `(` only after a space, so a bare
    // token compare is enough (`FUNCTION add(...)` tokenizes `FUNCTION` separately).
    next.eq_ignore_ascii_case("FUNCTION")
}

/// Whether `sql` begins `DROP FUNCTION …` (case-insensitive) (CONCEPT:EG-118).
fn is_drop_function(sql: &str) -> bool {
    let mut toks = sql.trim_start().split_whitespace();
    toks.next().is_some_and(|t| t.eq_ignore_ascii_case("DROP"))
        && toks.next().is_some_and(|t| t.eq_ignore_ascii_case("FUNCTION"))
}

/// Parse `CREATE [OR REPLACE] FUNCTION name(arg type, …) RETURNS <ret> AS $$ body $$
/// LANGUAGE sql` textually (CONCEPT:EG-118) into a [`CreateFunctionPlan`]. `<ret>` is a
/// scalar type, `TABLE(col type, …)`, or `SETOF type`; the `AS <body>` and `LANGUAGE
/// <lang>` clauses may appear in either order. Only `LANGUAGE sql` is implemented — a
/// procedural `LANGUAGE plpgsql` body is a documented follow-up and returns a precise
/// `Err`.
fn parse_create_function(sql: &str) -> Result<CreateFunctionPlan, String> {
    let s = sql.trim();
    let rest = strip_leading_kw(s, "CREATE")
        .ok_or("CREATE FUNCTION: expected `CREATE` (CONCEPT:EG-118)")?;
    let (or_replace, rest) = match strip_leading_kw(rest, "OR") {
        Some(r) => (
            true,
            strip_leading_kw(r, "REPLACE").ok_or("CREATE OR …: expected `REPLACE`")?,
        ),
        None => (false, rest),
    };
    let rest = strip_leading_kw(rest, "FUNCTION").ok_or("expected `FUNCTION` keyword")?;

    // Function name up to the argument-list `(`.
    let paren = rest
        .find('(')
        .ok_or("CREATE FUNCTION requires a parenthesized argument list")?;
    let name = last_ident_str(rest[..paren].trim());
    if name.is_empty() {
        return Err("CREATE FUNCTION requires a function name".to_string());
    }

    // Argument list (matching paren; types may themselves carry parens like `numeric(10,2)`).
    let (args_inner, after_args) = read_balanced_parens(rest, paren)
        .ok_or("CREATE FUNCTION argument list is not balanced")?;
    let args = parse_arg_defs(args_inner, "argument")?;

    let after = rest[after_args..].trim_start();
    let after = strip_leading_kw(after, "RETURNS")
        .ok_or("CREATE FUNCTION requires a `RETURNS` clause")?;
    let (returns, after_ret) = parse_returns(after)?;

    let (body, language) = parse_body_and_language(after_ret)?;
    if !language.eq_ignore_ascii_case("sql") {
        return Err(format!(
            "CREATE FUNCTION LANGUAGE `{language}` is not implemented — only LANGUAGE sql is \
             supported; a procedural PL/pgSQL body (IF/LOOP/variables/RETURN) is a documented \
             follow-up (CONCEPT:EG-118)"
        ));
    }
    let body = body.trim().to_string();
    if body.is_empty() {
        return Err("CREATE FUNCTION body is empty".to_string());
    }
    Ok(CreateFunctionPlan {
        func: StoredFunction {
            name,
            args,
            returns,
            body,
        },
        or_replace,
    })
}

/// Parse `DROP FUNCTION [IF EXISTS] name [(argtypes…)] [CASCADE|RESTRICT]` textually
/// (CONCEPT:EG-118). The optional argument-type signature and CASCADE/RESTRICT are
/// parsed and ignored (functions are keyed by name; overloading is a follow-up).
fn parse_drop_function(sql: &str) -> Result<DropFunctionPlan, String> {
    let s = sql.trim().trim_end_matches(';').trim();
    let rest = strip_leading_kw(s, "DROP").ok_or("DROP FUNCTION: expected `DROP`")?;
    let rest = strip_leading_kw(rest, "FUNCTION").ok_or("expected `FUNCTION` keyword")?;
    let (if_exists, rest) = match strip_leading_kw(rest, "IF") {
        Some(r) => (
            true,
            strip_leading_kw(r, "EXISTS").ok_or("DROP FUNCTION IF …: expected `EXISTS`")?,
        ),
        None => (false, rest),
    };
    // Name runs until whitespace, `(` (an argument-type signature), or end.
    let end = rest
        .find(|c: char| c.is_whitespace() || c == '(')
        .unwrap_or(rest.len());
    let name = last_ident_str(rest[..end].trim());
    if name.is_empty() {
        return Err("DROP FUNCTION requires a function name".to_string());
    }
    Ok(DropFunctionPlan { name, if_exists })
}

/// Strip a leading whole-word keyword (case-insensitive) from `s`, returning the
/// remaining text trimmed of leading whitespace, or `None` if `s` does not start with
/// `kw` on a word boundary.
fn strip_leading_kw<'a>(s: &'a str, kw: &str) -> Option<&'a str> {
    let s = s.trim_start();
    if s.len() >= kw.len() && s[..kw.len()].eq_ignore_ascii_case(kw) {
        let after = &s[kw.len()..];
        // Word boundary: the char after the keyword must not continue an identifier.
        if after
            .chars()
            .next()
            .is_none_or(|c| !c.is_alphanumeric() && c != '_')
        {
            return Some(after.trim_start());
        }
    }
    None
}

/// The last (unqualified) segment of a possibly schema-qualified name, with surrounding
/// double-quotes stripped (`public."Add"` → `Add`, `add` → `add`).
fn last_ident_str(name: &str) -> String {
    name.rsplit('.')
        .next()
        .unwrap_or(name)
        .trim()
        .trim_matches('"')
        .to_string()
}

/// Read a balanced `(…)` group whose opening paren is at byte `open` in `s`. Returns the
/// INNER text (between the parens) and the byte index just past the matching `)`. Skips
/// `'…'` string literals and `$$…$$` dollar bodies so a paren inside them does not
/// unbalance the count.
fn read_balanced_parens(s: &str, open: usize) -> Option<(&str, usize)> {
    let bytes = s.as_bytes();
    if *bytes.get(open)? != b'(' {
        return None;
    }
    let mut depth = 0usize;
    let mut i = open;
    let mut in_squote = false;
    while i < bytes.len() {
        let c = bytes[i];
        if in_squote {
            if c == b'\'' {
                in_squote = false;
            }
            i += 1;
            continue;
        }
        match c {
            b'\'' => in_squote = true,
            b'$' => {
                // A dollar-quoted body — skip it wholesale.
                if let Some((_, end)) = super::pgfamily::read_dollar_quoted(s, i) {
                    i = end;
                    continue;
                }
            }
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some((&s[open + 1..i], i + 1));
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Split `inner` (the text between an argument list's parens) into `name type` defs at
/// top-level commas (CONCEPT:EG-118). Empty (or whitespace-only) `inner` ⇒ no args. Each
/// entry's FIRST token is the name and the REST is the type spelling (so `n numeric(10,2)`
/// and `ts double precision` decode correctly). `what` names the context for errors.
fn parse_arg_defs(inner: &str, what: &str) -> Result<Vec<CatalogArg>, String> {
    let mut out = Vec::new();
    for piece in split_top_level_commas(inner) {
        let piece = piece.trim();
        if piece.is_empty() {
            continue;
        }
        let mut it = piece.splitn(2, char::is_whitespace);
        let name = it.next().unwrap_or("").trim().trim_matches('"');
        let type_name = it.next().map(|t| t.trim()).unwrap_or("");
        if name.is_empty() || type_name.is_empty() {
            return Err(format!(
                "CREATE FUNCTION {what} `{piece}` must be `name type` (CONCEPT:EG-118)"
            ));
        }
        out.push(CatalogArg {
            name: name.to_string(),
            type_name: type_name.to_string(),
        });
    }
    Ok(out)
}

/// Split `s` at commas that are NOT nested inside parens / `'…'` / `$$…$$`.
fn split_top_level_commas(s: &str) -> Vec<&str> {
    let bytes = s.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut in_squote = false;
    let mut start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        if in_squote {
            if c == b'\'' {
                in_squote = false;
            }
            i += 1;
            continue;
        }
        match c {
            b'\'' => in_squote = true,
            b'$' => {
                if let Some((_, end)) = super::pgfamily::read_dollar_quoted(s, i) {
                    i = end;
                    continue;
                }
            }
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    parts.push(&s[start..]);
    parts
}

/// Parse a `RETURNS` spec at the START of `after` (CONCEPT:EG-118): `TABLE(col type, …)`,
/// `SETOF <type>`, or a scalar `<type>`. Returns the decoded [`FunctionReturns`] and the
/// remaining text (the `AS`/`LANGUAGE` tail).
fn parse_returns(after: &str) -> Result<(FunctionReturns, &str), String> {
    if let Some(rest) = strip_leading_kw(after, "TABLE") {
        let rest = rest.trim_start();
        let open = rest
            .find('(')
            .ok_or("RETURNS TABLE requires a `(col type, …)` column list")?;
        let (inner, end) =
            read_balanced_parens(rest, open).ok_or("RETURNS TABLE column list is not balanced")?;
        let cols = parse_arg_defs(inner, "RETURNS TABLE column")?;
        return Ok((FunctionReturns::Table(cols), rest[end..].trim_start()));
    }
    if let Some(rest) = strip_leading_kw(after, "SETOF") {
        let (ty, tail) = read_return_type_word(rest)?;
        return Ok((FunctionReturns::SetOf(ty), tail));
    }
    // A scalar return type: everything up to the first top-level `AS` / `LANGUAGE`.
    let end = [
        find_top_level_kw(after, "AS"),
        find_top_level_kw(after, "LANGUAGE"),
    ]
    .into_iter()
    .flatten()
    .min()
    .ok_or("CREATE FUNCTION requires an `AS <body>` clause")?;
    let ty = after[..end].trim();
    if ty.is_empty() {
        return Err("RETURNS requires a return type".to_string());
    }
    Ok((FunctionReturns::Scalar(ty.to_string()), after[end..].trim_start()))
}

/// Read a single whitespace-delimited return-type word (e.g. after `SETOF`) and return it
/// plus the remaining tail.
fn read_return_type_word(s: &str) -> Result<(String, &str), String> {
    let s = s.trim_start();
    let end = s.find(char::is_whitespace).unwrap_or(s.len());
    let ty = s[..end].trim();
    if ty.is_empty() {
        return Err("SETOF requires a type".to_string());
    }
    Ok((ty.to_string(), s[end..].trim_start()))
}

/// Parse the `AS <body>` + `LANGUAGE <lang>` tail (CONCEPT:EG-118), in EITHER order.
/// `<body>` is a `$$…$$`/`$tag$…$tag$` dollar body or a `'…'` single-quoted string.
/// Returns `(body, language)`.
fn parse_body_and_language(tail: &str) -> Result<(String, String), String> {
    let mut body: Option<String> = None;
    let mut language: Option<String> = None;
    let mut cur = tail.trim_start();
    // Up to two clauses (`AS` and `LANGUAGE`), any order.
    for _ in 0..2 {
        if let Some(rest) = strip_leading_kw(cur, "AS") {
            let (b, next) = read_function_body(rest)?;
            body = Some(b);
            cur = next.trim_start();
        } else if let Some(rest) = strip_leading_kw(cur, "LANGUAGE") {
            let rest = rest.trim_start();
            let end = rest
                .find(|c: char| c.is_whitespace() || c == ';')
                .unwrap_or(rest.len());
            language = Some(rest[..end].trim().to_string());
            cur = rest[end..].trim_start();
        } else {
            break;
        }
    }
    match (body, language) {
        (Some(b), Some(l)) => Ok((b, l)),
        (Some(_), None) => Err("CREATE FUNCTION requires a `LANGUAGE` clause".to_string()),
        _ => Err("CREATE FUNCTION requires an `AS <body>` clause".to_string()),
    }
}

/// Read a function body at the START of `s`: a `$$…$$`/`$tag$…$tag$` dollar body or a
/// `'…'` single-quoted string. Returns the body text and the remaining tail.
fn read_function_body(s: &str) -> Result<(String, &str), String> {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    match bytes.first() {
        Some(b'$') => {
            let (body, end) = super::pgfamily::read_dollar_quoted(s, 0)
                .ok_or("CREATE FUNCTION dollar-quoted body is not closed")?;
            Ok((body, s[end..].trim_start()))
        }
        Some(b'\'') => {
            // A single-quoted body; `''` is an escaped quote.
            let mut i = 1usize;
            let mut buf = String::new();
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if bytes.get(i + 1) == Some(&b'\'') {
                        buf.push('\'');
                        i += 2;
                        continue;
                    }
                    return Ok((buf, s[i + 1..].trim_start()));
                }
                buf.push(bytes[i] as char);
                i += 1;
            }
            Err("CREATE FUNCTION single-quoted body is not closed".to_string())
        }
        _ => Err("CREATE FUNCTION `AS` must be followed by a `$$…$$` or '…' body".to_string()),
    }
}

/// Find the byte index of the first whole-word, case-insensitive occurrence of `kw` in
/// `s` at TOP LEVEL — not inside `'…'`, `$$…$$`, or `(…)` (CONCEPT:EG-118). Returns
/// `None` if absent.
fn find_top_level_kw(s: &str, kw: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let kb = kw.as_bytes();
    let mut depth = 0usize;
    let mut in_squote = false;
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        if in_squote {
            if c == b'\'' {
                in_squote = false;
            }
            i += 1;
            continue;
        }
        match c {
            b'\'' => {
                in_squote = true;
                i += 1;
                continue;
            }
            b'$' => {
                if let Some((_, end)) = super::pgfamily::read_dollar_quoted(s, i) {
                    i = end;
                    continue;
                }
            }
            b'(' => {
                depth += 1;
                i += 1;
                continue;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                i += 1;
                continue;
            }
            _ => {}
        }
        if depth == 0
            && i + kb.len() <= bytes.len()
            && s[i..i + kb.len()].eq_ignore_ascii_case(kw)
        {
            let before_ok = i == 0 || !is_ident_byte(bytes[i - 1]);
            let after_ok =
                i + kb.len() == bytes.len() || !is_ident_byte(bytes[i + kb.len()]);
            if before_ok && after_ok {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

/// Whether `b` continues a SQL identifier (`[A-Za-z0-9_]`).
fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Decode `ALTER TABLE name ADD COLUMN col type`. Only a single `ADD COLUMN`
/// operation is supported in this increment; other ALTER ops are explicit follow-ups.
fn classify_alter_table(
    name: &ObjectName,
    operations: &[AlterTableOperation],
) -> Result<AlterTablePlan, String> {
    let table = last_ident(name);
    if is_reserved_table(&table) {
        return Err(format!("cannot ALTER the reserved graph table `{table}`"));
    }
    let op = match operations {
        [one] => one,
        _ => return Err("ALTER TABLE supports exactly one ADD COLUMN per statement".to_string()),
    };
    match op {
        AlterTableOperation::AddColumn { column_def, .. } => Ok(AlterTablePlan {
            name: table,
            add_column: decode_column_def(column_def)?,
        }),
        other => Err(format!(
            "ALTER TABLE supports only ADD COLUMN in this increment, got `{other}`"
        )),
    }
}

/// Decode a WHERE clause into the single simple-equality predicate the wire DML
/// path can resolve. A missing WHERE is rejected (no unscoped mass UPDATE/DELETE).
fn decode_where(selection: Option<&Expr>, verb: &str) -> Result<WhereEq, String> {
    let expr = selection.ok_or_else(|| {
        format!(
            "{verb} nodes requires a WHERE clause (an unscoped {verb} is refused; \
             use `WHERE id = '…'` or `WHERE <prop> = <literal>`)"
        )
    })?;
    // The `id = <literal>` fast path stays a single-node address.
    if let Expr::BinaryOp { left, op, right } = expr {
        if *op == BinaryOperator::Eq {
            if let Ok(column) = ident_column(left) {
                if column.eq_ignore_ascii_case("id") {
                    let value = expr_to_json(right)?;
                    return Ok(WhereEq::Id(scalar_id(value)?));
                }
            }
        }
    }
    // CONCEPT:EG-045 — any other WHERE decodes to a serializable compound predicate.
    // `where_sql` is re-run through the read path to resolve candidate ids; `pred`
    // is re-checked under the write guard for serializable semantics.
    let pred = decode_predicate(expr)?;
    Ok(WhereEq::Predicate {
        where_sql: expr.to_string(),
        pred,
    })
}

/// Decode a sqlparser `Expr` WHERE tree into a serializable [`eg_types::RowPredicate`]
/// (CONCEPT:EG-045). Supports `AND`/`OR`, the six scalar comparisons, `NOT`, `IN`,
/// `BETWEEN`, `IS [NOT] NULL`, and parenthesised nesting. The left operand of a
/// comparison/`IN`/`BETWEEN`/`IS NULL` must be a (possibly qualified) column; the
/// right operands must be literals (reusing `ident_column` + `expr_to_json`).
fn decode_predicate(expr: &Expr) -> Result<eg_types::RowPredicate, String> {
    use datafusion::sql::sqlparser::ast::UnaryOperator;
    use eg_types::{CmpOp, RowPredicate};
    match expr {
        Expr::Nested(inner) => decode_predicate(inner),
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(RowPredicate::And(vec![
                decode_predicate(left)?,
                decode_predicate(right)?,
            ])),
            BinaryOperator::Or => Ok(RowPredicate::Or(vec![
                decode_predicate(left)?,
                decode_predicate(right)?,
            ])),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq => {
                let col = ident_column(left)?;
                let value = expr_to_json(right)?;
                let op = match op {
                    BinaryOperator::Eq => CmpOp::Eq,
                    BinaryOperator::NotEq => CmpOp::Ne,
                    BinaryOperator::Lt => CmpOp::Lt,
                    BinaryOperator::LtEq => CmpOp::Le,
                    BinaryOperator::Gt => CmpOp::Gt,
                    BinaryOperator::GtEq => CmpOp::Ge,
                    _ => unreachable!(),
                };
                Ok(RowPredicate::Cmp { col, op, value })
            }
            other => Err(format!("unsupported operator in WHERE: `{other}`")),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => Ok(RowPredicate::Not(Box::new(decode_predicate(expr)?))),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let col = ident_column(expr)?;
            let values = list
                .iter()
                .map(expr_to_json)
                .collect::<Result<Vec<_>, _>>()?;
            let pred = RowPredicate::In { col, values };
            Ok(if *negated {
                RowPredicate::Not(Box::new(pred))
            } else {
                pred
            })
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let col = ident_column(expr)?;
            let low = expr_to_json(low)?;
            let high = expr_to_json(high)?;
            let pred = RowPredicate::Between { col, low, high };
            Ok(if *negated {
                RowPredicate::Not(Box::new(pred))
            } else {
                pred
            })
        }
        Expr::IsNull(inner) => Ok(RowPredicate::IsNull {
            col: ident_column(inner)?,
        }),
        Expr::IsNotNull(inner) => Ok(RowPredicate::IsNotNull {
            col: ident_column(inner)?,
        }),
        other => Err(format!(
            "unsupported WHERE predicate (CONCEPT:EG-045 supports AND/OR/NOT/IN/\
             BETWEEN/comparisons/IS [NOT] NULL): `{other}`"
        )),
    }
}

/// Extract the bare column name from the left side of a WHERE equality. Accepts
/// an unqualified `col` or a qualified `nodes.col` (last segment wins).
fn ident_column(expr: &Expr) -> Result<String, String> {
    match expr {
        Expr::Identifier(id) => Ok(id.value.clone()),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|i| i.value.clone())
            .ok_or_else(|| "empty qualified column in WHERE".to_string()),
        other => Err(format!("WHERE left side must be a column, got `{other}`")),
    }
}

/// The last segment of a (possibly qualified) object name.
fn last_ident(name: &ObjectName) -> String {
    name.0
        .last()
        .map(|i| i.value.clone())
        .unwrap_or_else(|| name.to_string())
}

/// A scalar JSON value coerced to the string node-id form the engine stores.
fn scalar_id(val: Value) -> Result<String, String> {
    match val {
        Value::String(s) => Ok(s),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        other => Err(format!("`id` must be a scalar, got {other}")),
    }
}

/// Verify a write targets the `nodes` table (qualified or bare, last segment).
fn require_nodes_table(table: &str, verb: &str) -> Result<(), String> {
    let leaf = table.rsplit('.').next().unwrap_or(table);
    if leaf.eq_ignore_ascii_case("nodes") {
        Ok(())
    } else {
        Err(format!(
            "{verb} is only supported on the `nodes` table, got `{table}`"
        ))
    }
}

/// Verify an UPDATE/DELETE target is the bare `nodes` table with no join.
fn require_nodes_target(
    target: &datafusion::sql::sqlparser::ast::TableWithJoins,
    verb: &str,
) -> Result<(), String> {
    // CONCEPT:EG-047 — a JOIN on the target routes to the multi-table path before this
    // check; the plain path never carries joins.
    match &target.relation {
        TableFactor::Table { name, .. } => require_nodes_table(&name.to_string(), verb),
        _ => Err(format!("{verb} target must be the `nodes` table")),
    }
}

/// Decode `UPDATE nodes SET c = e[, …] FROM <other> WHERE …` into an
/// [`UpdateNodesJoin`] (CONCEPT:EG-047). Renders a resolver SELECT projecting the node
/// `id` plus each SET value expression (aliased to its target column) over the target +
/// FROM relations and the WHERE predicate, so the executor can read `(id, <set-cols…>)`
/// through DataFusion and apply each row.
fn classify_update_nodes_join(
    table: &TableWithJoins,
    assignments: &[Assignment],
    from: Option<&TableWithJoins>,
    selection: Option<&Expr>,
    returning: &Option<Vec<SelectItem>>,
) -> Result<UpdateNodesJoin, String> {
    if assignments.is_empty() {
        return Err("UPDATE nodes requires at least one SET assignment".to_string());
    }
    let mut set_targets = Vec::with_capacity(assignments.len());
    let mut projections = vec!["nodes.id AS id".to_string()];
    for a in assignments {
        let col = match &a.target {
            AssignmentTarget::ColumnName(name) => last_ident(name),
            AssignmentTarget::Tuple(_) => {
                return Err("UPDATE nodes tuple assignment is not supported".to_string())
            }
        };
        if col.eq_ignore_ascii_case("id") {
            return Err("UPDATE nodes cannot reassign the `id` column".to_string());
        }
        // Alias each SET value expression to its target column so the resolver row is
        // `(id, <target-col> = <value>…)`; quote the alias to survive reserved words.
        projections.push(format!("({}) AS \"{}\"", a.value, col));
        set_targets.push(col);
    }
    // FROM = the target `nodes` (with any joins) plus the correlated `FROM` relation.
    let mut from_sql = table.to_string();
    if let Some(f) = from {
        from_sql.push_str(", ");
        from_sql.push_str(&f.to_string());
    }
    let mut resolve_sql = format!("SELECT {} FROM {}", projections.join(", "), from_sql);
    if let Some(sel) = selection {
        resolve_sql.push_str(" WHERE ");
        resolve_sql.push_str(&sel.to_string());
    }
    Ok(UpdateNodesJoin {
        set_targets,
        resolve_sql,
        returning: returning.is_some(),
    })
}

/// Decode `DELETE FROM nodes USING <other> WHERE …` into a [`DeleteNodesJoin`]
/// (CONCEPT:EG-047). Renders a resolver SELECT projecting the node `id` over the target
/// + USING relations and the WHERE predicate.
fn classify_delete_nodes_join(
    target: &TableWithJoins,
    using: Option<&Vec<TableWithJoins>>,
    selection: Option<&Expr>,
    returning: &Option<Vec<SelectItem>>,
) -> Result<DeleteNodesJoin, String> {
    let mut from_sql = target.to_string();
    if let Some(tables) = using {
        for t in tables {
            from_sql.push_str(", ");
            from_sql.push_str(&t.to_string());
        }
    }
    let mut resolve_sql = format!("SELECT nodes.id AS id FROM {from_sql}");
    if let Some(sel) = selection {
        resolve_sql.push_str(" WHERE ");
        resolve_sql.push_str(&sel.to_string());
    }
    Ok(DeleteNodesJoin {
        resolve_sql,
        returning: returning.is_some(),
    })
}

/// A literal `VALUES`/`SET`/WHERE cell → a JSON value. Only SQL literals are
/// accepted (no expressions/functions), which keeps the write path a pure data
/// move with no evaluation. Parameter placeholders (`$N`) are not seen here — the
/// shim substitutes them to literals before classify.
fn expr_to_json(expr: &Expr) -> Result<Value, String> {
    match expr {
        Expr::Value(v) => sql_value_to_json(v),
        // `-1`, `+2.5` etc. — a unary op over a numeric literal.
        Expr::UnaryOp { op, expr } => {
            use datafusion::sql::sqlparser::ast::UnaryOperator;
            let inner = expr_to_json(expr)?;
            match (op, inner) {
                (UnaryOperator::Minus, Value::Number(n)) => {
                    if let Some(i) = n.as_i64() {
                        Ok(Value::Number((-i).into()))
                    } else if let Some(f) = n.as_f64() {
                        serde_json::Number::from_f64(-f)
                            .map(Value::Number)
                            .ok_or_else(|| "non-finite numeric literal".to_string())
                    } else {
                        Err("unsupported numeric literal".to_string())
                    }
                }
                (UnaryOperator::Plus, v @ Value::Number(_)) => Ok(v),
                _ => Err(format!("unsupported unary expression in VALUES: {expr}")),
            }
        }
        other => Err(format!("unsupported value expression: {other}")),
    }
}

fn sql_value_to_json(v: &SqlValue) -> Result<Value, String> {
    match v {
        SqlValue::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(Value::Number(i.into()))
            } else if let Ok(f) = n.parse::<f64>() {
                serde_json::Number::from_f64(f)
                    .map(Value::Number)
                    .ok_or_else(|| "non-finite numeric literal".to_string())
            } else {
                Err(format!("invalid numeric literal: {n}"))
            }
        }
        SqlValue::SingleQuotedString(s)
        | SqlValue::DoubleQuotedString(s)
        | SqlValue::EscapedStringLiteral(s) => Ok(Value::String(s.clone())),
        SqlValue::Boolean(b) => Ok(Value::Bool(*b)),
        SqlValue::Null => Ok(Value::Null),
        other => Err(format!("unsupported SQL literal: {other}")),
    }
}

// ── Postgres/Mongo JSON operator lowering onto `Pred::JsonPath` (CONCEPT:EG-084) ──
//
// DataFusion has no JSONPath, so the deep JSON operators are decoded HERE into the
// `Pred::JsonPath` wire predicate; `eg-plan`'s FILTER leg then evaluates them per-row
// (via `eg_core::jsonpath`) and the planner consults eg-core's inverted path-index for
// selectivity. All functions are pure decoders over the sqlparser `Expr` — no execution.

/// CONCEPT:EG-084 — lower a Postgres JSON WHERE expression onto a [`Pred::JsonPath`].
/// Recognizes:
///   * `col ->> 'k' = 'v'` / `col -> 'k' = <lit>` — deep equality over an accessor chain;
///   * `col #> '{a,b}' = <lit>` / `#>>` — path-array accessor equality;
///   * `col @> '<json>'` — `@>` JSON containment (at the accessor path; `$` for a bare col);
///   * `col @? '$.path'` — jsonpath existence;
///   * `jsonb_path_exists(col,'$.p')` / `jsonb_path_query(col,'$.p')` — existence;
///   * `jsonb_path_query(col,'$.p') = <lit>` — equality at the jsonpath.
///
/// Returns an `Err` for any expression that is not a lowerable JSON predicate (so a
/// caller can fall back to the plain relational path).
pub fn json_pred_from_expr(expr: &Expr) -> Result<Pred, String> {
    let inner = match expr {
        Expr::Nested(e) => e.as_ref(),
        other => other,
    };
    // A bare existence function: jsonb_path_exists/query(col, '$.path').
    if let Some((_, path)) = jsonb_fn_path(inner) {
        return Ok(Pred::JsonPath {
            path,
            op: JsonPathOp::Exists,
        });
    }
    if let Expr::BinaryOp { left, op, right } = inner {
        match op {
            // `col @> '<json>'` — containment at the accessor path (`$` for a bare column).
            BinaryOperator::AtArrow => {
                let path = json_accessor_path(left)
                    .map(|(_, p)| p)
                    .or_else(|| bare_column(left).map(|_| "$".to_string()))
                    .ok_or_else(|| {
                        format!("`@>` left must be a JSON column/accessor, got `{left}`")
                    })?;
                let value = json_literal(right)?;
                return Ok(Pred::JsonPath {
                    path,
                    op: JsonPathOp::Contains { value },
                });
            }
            // `col @? '$.path'` — the RHS is a full jsonpath string.
            BinaryOperator::AtQuestion => {
                let path = string_operand(right)
                    .ok_or_else(|| "`@?` right must be a jsonpath string literal".to_string())?;
                return Ok(Pred::JsonPath {
                    path,
                    op: JsonPathOp::Exists,
                });
            }
            // `<accessor> = <lit>` — deep equality. The LHS must be a `->`/`#>` accessor
            // chain or a `jsonb_path_query(...)` call (a bare `col = 'v'` is relational
            // and is NOT lowered here).
            BinaryOperator::Eq => {
                let path = json_accessor_path(left)
                    .or_else(|| jsonb_fn_path(left))
                    .map(|(_, p)| p)
                    .ok_or_else(|| format!("`=` left is not a JSON accessor, got `{left}`"))?;
                let value = expr_to_json(right)?;
                return Ok(Pred::JsonPath {
                    path,
                    op: JsonPathOp::Eq { value },
                });
            }
            _ => {}
        }
    }
    Err(format!("not a lowerable JSON predicate: `{expr}`"))
}

/// A bare (possibly qualified) column name, else `None`.
fn bare_column(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => ident_column(expr).ok(),
        Expr::Nested(e) => bare_column(e),
        _ => None,
    }
}

/// Build a JSONPath string from a Postgres accessor chain (`col -> 'a' ->> 'b'`,
/// `col #> '{a,b}'`), returning `(column, "$['a']['b']")` (CONCEPT:EG-084). Requires at
/// least one accessor operator — a bare column returns `None` (it is not a JSON access).
fn json_accessor_path(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::Nested(e) => json_accessor_path(e),
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Arrow | BinaryOperator::LongArrow => {
                let (col, base) = accessor_base(left)?;
                Some((col, format!("{base}{}", json_key_segment(right)?)))
            }
            BinaryOperator::HashArrow | BinaryOperator::HashLongArrow => {
                let (col, base) = accessor_base(left)?;
                Some((col, format!("{base}{}", json_text_path(right)?)))
            }
            _ => None,
        },
        _ => None,
    }
}

/// The base of an accessor chain: a bare column (`$`) or a nested accessor.
fn accessor_base(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            Some((ident_column(expr).ok()?, "$".to_string()))
        }
        Expr::Nested(e) => accessor_base(e),
        _ => json_accessor_path(expr),
    }
}

/// One `->`/`->>` key: a string literal ⇒ `['key']`, a number literal ⇒ `[n]`.
fn json_key_segment(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(SqlValue::SingleQuotedString(s))
        | Expr::Value(SqlValue::DoubleQuotedString(s)) => Some(format!("['{s}']")),
        Expr::Value(SqlValue::Number(n, _)) => {
            let i: usize = n.parse().ok()?;
            Some(format!("[{i}]"))
        }
        _ => None,
    }
}

/// A `#>` / `#>>` text path `'{a,b,1}'` ⇒ `['a']['b'][1]` (numeric tokens are indices).
fn json_text_path(expr: &Expr) -> Option<String> {
    let s = string_operand(expr)?;
    let inner = s.trim().strip_prefix('{')?.strip_suffix('}')?;
    let mut path = String::new();
    for tok in inner.split(',') {
        let tok = tok.trim();
        if tok.is_empty() {
            return None;
        }
        match tok.parse::<usize>() {
            Ok(i) => path.push_str(&format!("[{i}]")),
            Err(_) => path.push_str(&format!("['{tok}']")),
        }
    }
    Some(path)
}

/// Extract a string literal operand, else `None`.
fn string_operand(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(SqlValue::SingleQuotedString(s))
        | Expr::Value(SqlValue::DoubleQuotedString(s)) => Some(s.clone()),
        Expr::Nested(e) => string_operand(e),
        _ => None,
    }
}

/// The RHS of `@>`: a JSON literal (a quoted string parsed as JSON, or a bare scalar).
fn json_literal(expr: &Expr) -> Result<Value, String> {
    match expr {
        Expr::Value(SqlValue::SingleQuotedString(s))
        | Expr::Value(SqlValue::DoubleQuotedString(s)) => {
            serde_json::from_str(s).map_err(|e| format!("`@>` right must be a JSON literal: {e}"))
        }
        other => expr_to_json(other),
    }
}

/// Recognize `jsonb_path_query`/`jsonb_path_exists`/… `(col, '$.path')`, returning
/// `(column, jsonpath)` (CONCEPT:EG-084).
fn jsonb_fn_path(expr: &Expr) -> Option<(String, String)> {
    let Expr::Function(Function { name, args, .. }) = expr else {
        return None;
    };
    let fname = name.to_string().to_ascii_lowercase();
    if !matches!(
        fname.as_str(),
        "jsonb_path_query"
            | "json_path_query"
            | "jsonb_path_exists"
            | "jsonb_path_query_first"
            | "jsonb_path_match"
    ) {
        return None;
    }
    let FunctionArguments::List(list) = args else {
        return None;
    };
    let mut exprs = list.args.iter().filter_map(|a| match a {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
        _ => None,
    });
    let col = bare_column(exprs.next()?)?;
    let path = string_operand(exprs.next()?)?;
    Some((col, path))
}

/// CONCEPT:EG-084 — lower a Mongo-style `$match` document filter onto ANDed
/// [`Pred::JsonPath`]s. Each entry maps a dotted field path to either a bare value / an
/// `{ "$eq": v }` spec (equality), `{ "$exists": true }` (existence), or `{ "$contains":
/// v }` (a Mongo-ism for our `@>` containment). No Mongo/doc-query SURFACE exists in the
/// engine yet, so this is the additive lowering primitive a future `$match` entry point
/// would call — kept public + tested.
pub fn mongo_match_to_preds(filter: &Value) -> Result<Vec<Pred>, String> {
    let obj = filter
        .as_object()
        .ok_or_else(|| "$match filter must be a JSON object".to_string())?;
    let mut preds = Vec::with_capacity(obj.len());
    for (field, spec) in obj {
        let path = mongo_field_to_path(field);
        let pred = match spec {
            Value::Object(m) if m.keys().any(|k| k.starts_with('$')) => {
                if let Some(v) = m.get("$eq") {
                    Pred::JsonPath {
                        path,
                        op: JsonPathOp::Eq { value: v.clone() },
                    }
                } else if let Some(v) = m.get("$exists") {
                    if v.as_bool() == Some(true) {
                        Pred::JsonPath {
                            path,
                            op: JsonPathOp::Exists,
                        }
                    } else {
                        return Err("$match `$exists: false` is not supported".to_string());
                    }
                } else if let Some(v) = m.get("$contains") {
                    Pred::JsonPath {
                        path,
                        op: JsonPathOp::Contains { value: v.clone() },
                    }
                } else {
                    return Err(format!("unsupported $match operator spec in `{field}`"));
                }
            }
            other => Pred::JsonPath {
                path,
                op: JsonPathOp::Eq {
                    value: other.clone(),
                },
            },
        };
        preds.push(pred);
    }
    Ok(preds)
}

/// A Mongo dotted field path (`a.b`) ⇒ the JSONPath `$['a']['b']` (CONCEPT:EG-084).
fn mongo_field_to_path(field: &str) -> String {
    let mut p = String::from("$");
    for seg in field.split('.') {
        p.push_str(&format!("['{seg}']"));
    }
    p
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn param_site_select_against_column() {
        let sites = infer_param_sites("SELECT id FROM nodes WHERE rank > $1").unwrap();
        assert_eq!(sites, vec![ParamSite::Column("rank".to_string())]);
    }

    #[test]
    fn param_site_update_set_and_id() {
        let sites = infer_param_sites("UPDATE nodes SET rank = $1 WHERE id = $2").unwrap();
        assert_eq!(
            sites,
            vec![ParamSite::Column("rank".to_string()), ParamSite::IdColumn]
        );
    }

    #[test]
    fn param_site_delete_by_id() {
        let sites = infer_param_sites("DELETE FROM nodes WHERE id = $1").unwrap();
        assert_eq!(sites, vec![ParamSite::IdColumn]);
    }

    #[test]
    fn param_site_insert_values() {
        let sites = infer_param_sites("INSERT INTO nodes (id, rank) VALUES ($1, $2)").unwrap();
        assert_eq!(
            sites,
            vec![ParamSite::IdColumn, ParamSite::Column("rank".to_string())]
        );
    }

    #[test]
    fn param_site_no_params() {
        assert!(infer_param_sites("SELECT id FROM nodes")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn schema_probe_drops_where_and_limit() {
        let probe = schema_probe_sql("SELECT id FROM nodes WHERE rank = 5 ORDER BY id LIMIT 10")
            .unwrap()
            .to_ascii_uppercase();
        assert!(probe.contains("SELECT ID FROM NODES"), "{probe}");
        assert!(
            probe.contains("WHERE TRUE"),
            "predicate neutralized: {probe}"
        );
        assert!(!probe.contains("LIMIT"), "limit dropped: {probe}");
        // A non-SELECT yields None.
        assert!(schema_probe_sql("INSERT INTO nodes (id) VALUES ('n1')").is_none());
    }

    #[test]
    fn returning_columns_named() {
        assert_eq!(
            returning_columns("INSERT INTO nodes (id) VALUES ('n1') RETURNING id, rank"),
            Some(vec!["id".to_string(), "rank".to_string()])
        );
        // RETURNING * can't be named statically.
        assert_eq!(
            returning_columns("UPDATE nodes SET rank = 1 WHERE id = 'n1' RETURNING *"),
            None
        );
        // A plain write (no RETURNING) → None.
        assert_eq!(returning_columns("DELETE FROM nodes WHERE id = 'n1'"), None);
    }

    #[test]
    fn select_is_read() {
        assert_eq!(
            classify("SELECT id FROM nodes WHERE rank >= 2").unwrap(),
            StatementKind::Read
        );
        assert_eq!(
            classify("WITH x AS (SELECT 1) SELECT * FROM x").unwrap(),
            StatementKind::Read
        );
    }

    #[test]
    fn insert_node_decodes_id_and_props() {
        let k =
            classify("INSERT INTO nodes (id, type, rank, active) VALUES ('n1', 'Agent', 7, true)")
                .unwrap();
        let StatementKind::InsertNodes(ins) = k else {
            panic!("expected InsertNodes, got {k:?}");
        };
        assert_eq!(ins.rows.len(), 1);
        assert!(!ins.returning);
        let n = &ins.rows[0];
        assert_eq!(n.node_id, "n1");
        assert_eq!(n.properties.get("type").unwrap(), "Agent");
        assert_eq!(n.properties.get("rank").unwrap(), &Value::Number(7.into()));
        assert_eq!(n.properties.get("active").unwrap(), &Value::Bool(true));
    }

    #[test]
    fn insert_negative_number() {
        let k = classify("INSERT INTO nodes (id, delta) VALUES ('n1', -3)").unwrap();
        let StatementKind::InsertNodes(ins) = k else {
            panic!("expected InsertNodes");
        };
        assert_eq!(
            ins.rows[0].properties.get("delta").unwrap(),
            &Value::Number((-3).into())
        );
    }

    #[test]
    fn insert_multi_row() {
        let k = classify(
            "INSERT INTO nodes (id, type) VALUES ('a', 'Agent'), ('b', 'Tool'), ('c', 'Agent')",
        )
        .unwrap();
        let StatementKind::InsertNodes(ins) = k else {
            panic!("expected InsertNodes");
        };
        assert_eq!(ins.rows.len(), 3);
        assert_eq!(ins.rows[0].node_id, "a");
        assert_eq!(ins.rows[1].node_id, "b");
        assert_eq!(ins.rows[2].node_id, "c");
        assert_eq!(ins.rows[2].properties.get("type").unwrap(), "Agent");
    }

    #[test]
    fn insert_returning_flag() {
        let k =
            classify("INSERT INTO nodes (id, type) VALUES ('n1', 'Agent') RETURNING id").unwrap();
        let StatementKind::InsertNodes(ins) = k else {
            panic!("expected InsertNodes");
        };
        assert!(ins.returning);
    }

    #[test]
    fn update_by_id_decodes_set_and_selector() {
        let k = classify("UPDATE nodes SET rank = 5, status = 'done' WHERE id = 'n1'").unwrap();
        let StatementKind::UpdateNodes(u) = k else {
            panic!("expected UpdateNodes, got {k:?}");
        };
        assert_eq!(u.set.get("rank").unwrap(), &Value::Number(5.into()));
        assert_eq!(u.set.get("status").unwrap(), "done");
        assert_eq!(u.selector, WhereEq::Id("n1".to_string()));
        assert!(!u.returning);
    }

    #[test]
    fn update_by_property_selector() {
        let k = classify("UPDATE nodes SET active = false WHERE type = 'Tool'").unwrap();
        let StatementKind::UpdateNodes(u) = k else {
            panic!("expected UpdateNodes");
        };
        // CONCEPT:EG-045 — a non-id single-eq WHERE now decodes to a Predicate.
        let WhereEq::Predicate { pred, .. } = u.selector else {
            panic!("expected Predicate, got {:?}", u.selector);
        };
        assert_eq!(
            pred,
            eg_types::RowPredicate::Cmp {
                col: "type".to_string(),
                op: eg_types::CmpOp::Eq,
                value: Value::String("Tool".to_string()),
            }
        );
    }

    #[test]
    fn update_compound_where_decodes_predicate() {
        // CONCEPT:EG-045 — AND/OR/range now decode instead of being rejected.
        let k =
            classify("UPDATE nodes SET active = false WHERE rank > 2 AND type = 'Agent'").unwrap();
        let StatementKind::UpdateNodes(u) = k else {
            panic!("expected UpdateNodes");
        };
        let WhereEq::Predicate { where_sql, pred } = u.selector else {
            panic!("expected Predicate, got {:?}", u.selector);
        };
        assert!(
            where_sql.contains("rank") && where_sql.contains("type"),
            "{where_sql}"
        );
        assert_eq!(
            pred,
            eg_types::RowPredicate::And(vec![
                eg_types::RowPredicate::Cmp {
                    col: "rank".to_string(),
                    op: eg_types::CmpOp::Gt,
                    value: Value::Number(2.into()),
                },
                eg_types::RowPredicate::Cmp {
                    col: "type".to_string(),
                    op: eg_types::CmpOp::Eq,
                    value: Value::String("Agent".to_string()),
                },
            ])
        );
    }

    #[test]
    fn decode_predicate_in_between_or_not() {
        use eg_types::{CmpOp, RowPredicate};
        let StatementKind::DeleteNodes(d) =
            classify("DELETE FROM nodes WHERE type IN ('Tool', 'Skill')").unwrap()
        else {
            panic!("expected DeleteNodes");
        };
        let WhereEq::Predicate { pred, .. } = d.selector else {
            panic!("expected Predicate");
        };
        assert_eq!(
            pred,
            RowPredicate::In {
                col: "type".to_string(),
                values: vec![
                    Value::String("Tool".to_string()),
                    Value::String("Skill".to_string()),
                ],
            }
        );

        let StatementKind::UpdateNodes(u) =
            classify("UPDATE nodes SET active = false WHERE rank BETWEEN 2 AND 8").unwrap()
        else {
            panic!("expected UpdateNodes");
        };
        let WhereEq::Predicate { pred, .. } = u.selector else {
            panic!("expected Predicate");
        };
        assert_eq!(
            pred,
            RowPredicate::Between {
                col: "rank".to_string(),
                low: Value::Number(2.into()),
                high: Value::Number(8.into()),
            }
        );

        let StatementKind::UpdateNodes(u) =
            classify("UPDATE nodes SET active = false WHERE NOT (rank = 1 OR rank = 2)").unwrap()
        else {
            panic!("expected UpdateNodes");
        };
        let WhereEq::Predicate { pred, .. } = u.selector else {
            panic!("expected Predicate");
        };
        assert_eq!(
            pred,
            RowPredicate::Not(Box::new(RowPredicate::Or(vec![
                RowPredicate::Cmp {
                    col: "rank".to_string(),
                    op: CmpOp::Eq,
                    value: Value::Number(1.into()),
                },
                RowPredicate::Cmp {
                    col: "rank".to_string(),
                    op: CmpOp::Eq,
                    value: Value::Number(2.into()),
                },
            ])))
        );

        let StatementKind::DeleteNodes(d) =
            classify("DELETE FROM nodes WHERE note IS NULL").unwrap()
        else {
            panic!("expected DeleteNodes");
        };
        let WhereEq::Predicate { pred, .. } = d.selector else {
            panic!("expected Predicate");
        };
        assert_eq!(
            pred,
            RowPredicate::IsNull {
                col: "note".to_string()
            }
        );
    }

    #[test]
    fn update_returning_flag() {
        let k = classify("UPDATE nodes SET rank = 1 WHERE id = 'n1' RETURNING id, rank").unwrap();
        let StatementKind::UpdateNodes(u) = k else {
            panic!("expected UpdateNodes");
        };
        assert!(u.returning);
    }

    #[test]
    fn update_without_where_rejected() {
        let e = classify("UPDATE nodes SET rank = 1").unwrap_err();
        assert!(e.contains("requires a WHERE"), "{e}");
    }

    #[test]
    fn update_cannot_reassign_id() {
        let e = classify("UPDATE nodes SET id = 'x' WHERE id = 'n1'").unwrap_err();
        assert!(e.contains("cannot reassign the `id`"), "{e}");
    }

    #[test]
    fn update_unsupported_where_rejected() {
        // CONCEPT:EG-045 — function calls / non-literal RHS are still rejected.
        let e = classify("UPDATE nodes SET rank = 1 WHERE lower(type) = 'agent'").unwrap_err();
        assert!(
            e.contains("WHERE") || e.contains("column") || e.contains("value"),
            "{e}"
        );
    }

    #[test]
    fn delete_by_id() {
        let k = classify("DELETE FROM nodes WHERE id = 'n1'").unwrap();
        let StatementKind::DeleteNodes(d) = k else {
            panic!("expected DeleteNodes, got {k:?}");
        };
        assert_eq!(d.selector, WhereEq::Id("n1".to_string()));
        assert!(!d.returning);
    }

    #[test]
    fn delete_by_property_and_returning() {
        let k = classify("DELETE FROM nodes WHERE type = 'Tool' RETURNING id").unwrap();
        let StatementKind::DeleteNodes(d) = k else {
            panic!("expected DeleteNodes");
        };
        let WhereEq::Predicate { pred, .. } = d.selector else {
            panic!("expected Predicate");
        };
        assert_eq!(
            pred,
            eg_types::RowPredicate::Cmp {
                col: "type".to_string(),
                op: eg_types::CmpOp::Eq,
                value: Value::String("Tool".to_string()),
            }
        );
        assert!(d.returning);
    }

    #[test]
    fn delete_without_where_rejected() {
        let e = classify("DELETE FROM nodes").unwrap_err();
        assert!(e.contains("requires a WHERE"), "{e}");
    }

    #[test]
    fn insert_into_other_table_rejected() {
        let e = classify("INSERT INTO edges (src, dst) VALUES ('a', 'b')").unwrap_err();
        assert!(e.contains("only supported on the `nodes` table"), "{e}");
    }

    #[test]
    fn update_other_table_rejected() {
        let e = classify("UPDATE edges SET weight = 1 WHERE id = 'e1'").unwrap_err();
        assert!(e.contains("only supported on the `nodes` table"), "{e}");
    }

    #[test]
    fn insert_without_id_rejected() {
        let e = classify("INSERT INTO nodes (type) VALUES ('Agent')").unwrap_err();
        assert!(e.contains("must set the `id` column"), "{e}");
    }

    #[test]
    fn parse_error_surfaces() {
        assert!(classify("NOTAKEYWORD 1").is_err());
        assert!(classify("").is_err());
        assert!(classify("SELECT 1; SELECT 2").is_err());
    }

    // ── user-defined table DDL/DML (CONCEPT:EG-018) ───────────────────────────

    #[test]
    fn create_table_decodes_columns_and_options() {
        let k = classify(
            "CREATE TABLE IF NOT EXISTS prices (ts TIMESTAMP NOT NULL, symbol TEXT, \
             px DOUBLE, id BIGINT PRIMARY KEY)",
        )
        .unwrap();
        let StatementKind::CreateTable(p) = k else {
            panic!("expected CreateTable, got {k:?}");
        };
        assert!(p.if_not_exists);
        assert_eq!(p.name, "prices");
        assert_eq!(p.columns.len(), 4);
        assert!(!p.columns[0].nullable, "TIMESTAMP NOT NULL");
        assert!(p.columns[1].nullable, "TEXT defaults nullable");
        assert!(
            p.columns[3].primary_key && !p.columns[3].nullable,
            "PK ⇒ NOT NULL"
        );
    }

    #[test]
    fn create_table_rejects_reserved_names() {
        assert!(classify("CREATE TABLE nodes (id TEXT)").is_err());
        assert!(classify("CREATE TABLE edges (src TEXT)").is_err());
    }

    #[test]
    fn drop_and_alter_table_decode() {
        let StatementKind::DropTable(d) = classify("DROP TABLE IF EXISTS prices").unwrap() else {
            panic!("expected DropTable");
        };
        assert!(d.if_exists && d.name == "prices");

        let StatementKind::AlterTable(a) =
            classify("ALTER TABLE prices ADD COLUMN currency TEXT").unwrap()
        else {
            panic!("expected AlterTable");
        };
        assert_eq!(a.name, "prices");
        assert_eq!(a.add_column.name, "currency");
    }

    #[test]
    fn insert_into_user_table_multi_row() {
        let k = classify("INSERT INTO prices (symbol, px) VALUES ('AAPL', 1.5), ('MSFT', 2.5)")
            .unwrap();
        let StatementKind::InsertTable(ins) = k else {
            panic!("expected InsertTable, got {k:?}");
        };
        assert_eq!(ins.table, "prices");
        assert_eq!(ins.columns, vec!["symbol".to_string(), "px".to_string()]);
        assert_eq!(ins.rows.len(), 2);
        assert_eq!(ins.rows[1][0], Value::String("MSFT".into()));
    }

    #[test]
    fn insert_select_into_user_table() {
        let k = classify("INSERT INTO out (a) SELECT id FROM nodes").unwrap();
        let StatementKind::InsertSelect(ins) = k else {
            panic!("expected InsertSelect, got {k:?}");
        };
        assert_eq!(ins.table, "out");
        assert!(ins.select_sql.to_ascii_uppercase().contains("SELECT"));
    }

    #[test]
    fn update_delete_user_table_simple_where() {
        let StatementKind::UpdateTable(u) =
            classify("UPDATE prices SET px = 9.9 WHERE symbol = 'AAPL'").unwrap()
        else {
            panic!("expected UpdateTable");
        };
        assert_eq!(u.table, "prices");
        // CONCEPT:EG-045 — the user-table selector is now a RowPredicate.
        assert_eq!(
            u.selector.pred,
            eg_types::RowPredicate::Cmp {
                col: "symbol".to_string(),
                op: eg_types::CmpOp::Eq,
                value: Value::String("AAPL".into()),
            }
        );
        assert_eq!(u.set.get("px").unwrap(), &json_num(9.9));

        let StatementKind::DeleteTable(d) =
            classify("DELETE FROM prices WHERE symbol = 'MSFT'").unwrap()
        else {
            panic!("expected DeleteTable");
        };
        assert_eq!(
            d.selector.pred,
            eg_types::RowPredicate::Cmp {
                col: "symbol".to_string(),
                op: eg_types::CmpOp::Eq,
                value: Value::String("MSFT".into()),
            }
        );
    }

    #[test]
    fn update_user_table_requires_where() {
        assert!(classify("UPDATE prices SET px = 1").is_err());
        assert!(classify("DELETE FROM prices").is_err());
    }

    fn json_num(f: f64) -> Value {
        serde_json::Number::from_f64(f).map(Value::Number).unwrap()
    }

    // ── constraints, transactions, COPY (CONCEPT:EG-020) ──────────────────────

    #[test]
    fn create_table_decodes_constraints() {
        let StatementKind::CreateTable(p) = classify(
            "CREATE TABLE items (id BIGSERIAL PRIMARY KEY, sku TEXT UNIQUE, \
             qty INT DEFAULT 0 CHECK (qty >= 0))",
        )
        .unwrap() else {
            panic!("expected CreateTable");
        };
        // id: SERIAL + PK ⇒ serial, unique, not null.
        assert!(p.columns[0].serial && p.columns[0].primary_key && !p.columns[0].nullable);
        // sku: UNIQUE.
        assert!(p.columns[1].unique && !p.columns[1].primary_key);
        // qty: DEFAULT 0 + CHECK (qty >= 0).
        assert_eq!(p.columns[2].default, Some(Value::Number(0.into())));
        let chk = p.columns[2].check.clone().expect("check");
        assert_eq!(chk.op, CmpOp::Ge);
        assert_eq!(chk.value, Value::Number(0.into()));
    }

    #[test]
    fn default_nextval_is_serial() {
        let StatementKind::CreateTable(p) =
            classify("CREATE TABLE s (id INT DEFAULT nextval('s_id_seq'))").unwrap()
        else {
            panic!("expected CreateTable");
        };
        assert!(p.columns[0].serial, "DEFAULT nextval ⇒ SERIAL");
    }

    #[test]
    fn transactions_classify() {
        assert_eq!(classify("BEGIN").unwrap(), StatementKind::Begin);
        assert_eq!(classify("START TRANSACTION").unwrap(), StatementKind::Begin);
        assert_eq!(classify("COMMIT").unwrap(), StatementKind::Commit);
        assert_eq!(classify("ROLLBACK").unwrap(), StatementKind::Rollback);
    }

    #[test]
    fn copy_from_stdin_classifies() {
        let StatementKind::CopyIn(plan) =
            classify("COPY prices (symbol, px) FROM STDIN WITH (FORMAT csv, HEADER)").unwrap()
        else {
            panic!("expected CopyIn");
        };
        assert_eq!(plan.table, "prices");
        assert_eq!(plan.columns, vec!["symbol".to_string(), "px".to_string()]);
        assert_eq!(plan.format, CopyFormat::Csv);
        assert!(plan.header);

        // Default (no WITH) ⇒ TEXT.
        let StatementKind::CopyIn(t) = classify("COPY prices FROM STDIN").unwrap() else {
            panic!("expected CopyIn");
        };
        assert_eq!(t.format, CopyFormat::Text);
        assert!(t.columns.is_empty());

        // Binary format.
        let StatementKind::CopyIn(b) =
            classify("COPY prices FROM STDIN WITH (FORMAT binary)").unwrap()
        else {
            panic!("expected CopyIn");
        };
        assert_eq!(b.format, CopyFormat::Binary);
    }

    #[test]
    fn copy_to_and_reserved_rejected() {
        assert!(classify("COPY nodes FROM STDIN").is_err());
        assert!(classify("COPY prices TO STDOUT").is_err());
    }

    // ── INSERT INTO nodes … SELECT (CONCEPT:EG-046) ───────────────────────────

    #[test]
    fn insert_nodes_select_classifies() {
        let k = classify("INSERT INTO nodes (id, rank) SELECT sku, px FROM prices").unwrap();
        let StatementKind::InsertNodesSelect(ins) = k else {
            panic!("expected InsertNodesSelect, got {k:?}");
        };
        assert_eq!(ins.columns, vec!["id".to_string(), "rank".to_string()]);
        assert!(ins.select_sql.to_ascii_uppercase().contains("SELECT"));
        assert!(!ins.returning);
        assert!(ins.on_conflict.is_none());
    }

    #[test]
    fn insert_nodes_select_requires_id_column() {
        // Missing `id` in the column list → rejected.
        let e = classify("INSERT INTO nodes (rank) SELECT px FROM prices").unwrap_err();
        assert!(e.contains("must include `id`"), "{e}");
    }

    #[test]
    fn insert_nodes_values_still_classifies_as_insert_nodes() {
        // A literal VALUES body is NOT routed to the SELECT path.
        let k = classify("INSERT INTO nodes (id, rank) VALUES ('n1', 3)").unwrap();
        assert!(matches!(k, StatementKind::InsertNodes(_)), "{k:?}");
    }

    // ── ON CONFLICT + user-table RETURNING (CONCEPT:EG-048) ───────────────────

    #[test]
    fn insert_nodes_on_conflict_do_nothing() {
        let k = classify("INSERT INTO nodes (id, rank) VALUES ('n1', 3) ON CONFLICT (id) DO NOTHING")
            .unwrap();
        let StatementKind::InsertNodes(ins) = k else {
            panic!("expected InsertNodes");
        };
        let oc = ins.on_conflict.expect("on_conflict");
        assert_eq!(oc.target_cols, vec!["id".to_string()]);
        assert_eq!(oc.action, OnConflictAction::DoNothing);
    }

    #[test]
    fn insert_nodes_on_conflict_do_update() {
        let k = classify(
            "INSERT INTO nodes (id, rank) VALUES ('n1', 3) \
             ON CONFLICT (id) DO UPDATE SET rank = 9",
        )
        .unwrap();
        let StatementKind::InsertNodes(ins) = k else {
            panic!("expected InsertNodes");
        };
        let OnConflictAction::DoUpdate(set) = ins.on_conflict.unwrap().action else {
            panic!("expected DoUpdate");
        };
        assert_eq!(set.get("rank").unwrap(), &Value::Number(9.into()));
    }

    #[test]
    fn insert_user_table_on_conflict_and_returning() {
        let k = classify(
            "INSERT INTO prices (sku, px) VALUES ('AAPL', 1.5) \
             ON CONFLICT (sku) DO UPDATE SET px = 2.0 RETURNING sku",
        )
        .unwrap();
        let StatementKind::InsertTable(ins) = k else {
            panic!("expected InsertTable, got {k:?}");
        };
        assert!(ins.returning);
        let oc = ins.on_conflict.expect("on_conflict");
        assert_eq!(oc.target_cols, vec!["sku".to_string()]);
        assert!(matches!(oc.action, OnConflictAction::DoUpdate(_)));
    }

    #[test]
    fn on_conflict_do_update_where_rejected() {
        let e = classify(
            "INSERT INTO nodes (id) VALUES ('n1') ON CONFLICT (id) DO UPDATE SET rank = 1 WHERE rank > 0",
        )
        .unwrap_err();
        assert!(e.contains("WHERE"), "{e}");
    }

    // ── UPDATE…FROM / DELETE…USING on nodes (CONCEPT:EG-047) ──────────────────

    #[test]
    fn update_nodes_from_classifies_join() {
        let k = classify(
            "UPDATE nodes SET rank = p.px FROM prices p WHERE nodes.symbol = p.sku",
        )
        .unwrap();
        let StatementKind::UpdateNodesJoin(u) = k else {
            panic!("expected UpdateNodesJoin, got {k:?}");
        };
        assert_eq!(u.set_targets, vec!["rank".to_string()]);
        let up = u.resolve_sql.to_ascii_uppercase();
        assert!(up.contains("NODES.ID AS ID"), "{}", u.resolve_sql);
        assert!(up.contains("FROM NODES") && up.contains("PRICES"), "{}", u.resolve_sql);
        assert!(up.contains("WHERE"), "{}", u.resolve_sql);
        assert!(u.resolve_sql.contains("\"rank\""), "{}", u.resolve_sql);
    }

    #[test]
    fn update_nodes_join_cannot_reassign_id() {
        let e = classify("UPDATE nodes SET id = p.x FROM other p WHERE nodes.a = p.a").unwrap_err();
        assert!(e.contains("cannot reassign the `id`"), "{e}");
    }

    #[test]
    fn delete_nodes_using_classifies_join() {
        let k =
            classify("DELETE FROM nodes USING prices p WHERE nodes.symbol = p.sku").unwrap();
        let StatementKind::DeleteNodesJoin(d) = k else {
            panic!("expected DeleteNodesJoin, got {k:?}");
        };
        let up = d.resolve_sql.to_ascii_uppercase();
        assert!(up.contains("SELECT NODES.ID AS ID FROM NODES"), "{}", d.resolve_sql);
        assert!(up.contains("PRICES") && up.contains("WHERE"), "{}", d.resolve_sql);
    }

    #[test]
    fn plain_update_delete_nodes_unchanged() {
        // No FROM/USING → the simple single-table path (not the join path).
        assert!(matches!(
            classify("UPDATE nodes SET rank = 1 WHERE id = 'n1'").unwrap(),
            StatementKind::UpdateNodes(_)
        ));
        assert!(matches!(
            classify("DELETE FROM nodes WHERE id = 'n1'").unwrap(),
            StatementKind::DeleteNodes(_)
        ));
    }

    // ── CREATE VIEW / DROP VIEW (CONCEPT:EG-072) ──────────────────────────────

    #[test]
    fn create_and_drop_view_classify() {
        let k = classify("CREATE VIEW agents AS SELECT id FROM nodes WHERE type = 'Agent'").unwrap();
        let StatementKind::CreateView(v) = k else {
            panic!("expected CreateView, got {k:?}");
        };
        assert_eq!(v.name, "agents");
        assert!(!v.or_replace);
        assert!(v.select_sql.to_ascii_uppercase().contains("SELECT ID FROM NODES"));

        let StatementKind::CreateView(v2) =
            classify("CREATE OR REPLACE VIEW agents AS SELECT id FROM nodes").unwrap()
        else {
            panic!("expected CreateView");
        };
        assert!(v2.or_replace);

        let StatementKind::DropView(d) = classify("DROP VIEW IF EXISTS agents").unwrap() else {
            panic!("expected DropView");
        };
        assert!(d.if_exists && d.name == "agents");
    }

    #[test]
    fn create_view_rejects_reserved_and_materialized() {
        assert!(classify("CREATE VIEW nodes AS SELECT 1").is_err());
        assert!(classify("CREATE MATERIALIZED VIEW v AS SELECT 1").is_err());
    }

    // ── CREATE / DROP EXTENSION (CONCEPT:EG-102) ──────────────────────────────

    #[test]
    fn create_extension_classify() {
        let k = classify("CREATE EXTENSION vector").unwrap();
        let StatementKind::CreateExtension { name, if_not_exists } = k else {
            panic!("expected CreateExtension, got {k:?}");
        };
        assert_eq!(name, "vector");
        assert!(!if_not_exists);

        let k = classify("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public").unwrap();
        let StatementKind::CreateExtension { name, if_not_exists } = k else {
            panic!("expected CreateExtension");
        };
        assert_eq!(name, "vector");
        assert!(if_not_exists);

        // The other recognized names are accepted so a client's setup script proceeds.
        for ext in ["age", "pg_age", "timescaledb", "pg_search"] {
            assert!(
                matches!(
                    classify(&format!("CREATE EXTENSION IF NOT EXISTS {ext}")).unwrap(),
                    StatementKind::CreateExtension { .. }
                ),
                "extension `{ext}` should be recognized"
            );
        }
    }

    #[test]
    fn create_extension_rejects_unknown() {
        assert!(classify("CREATE EXTENSION nonesuch").is_err());
    }

    #[test]
    fn drop_extension_classify() {
        let StatementKind::DropExtension { name, if_exists } =
            classify("DROP EXTENSION IF EXISTS vector").unwrap()
        else {
            panic!("expected DropExtension");
        };
        assert_eq!(name, "vector");
        assert!(if_exists);

        let StatementKind::DropExtension { name, if_exists } =
            classify("DROP EXTENSION vector CASCADE").unwrap()
        else {
            panic!("expected DropExtension");
        };
        assert_eq!(name, "vector");
        assert!(!if_exists);
    }

    // ── Postgres-family extension parity routing (EG-114/116/117/119) ─────────

    #[test]
    fn eg114_classify_routes_cypher_call() {
        let k = classify(
            "SELECT * FROM cypher('g', $$ MATCH (n) RETURN n.id $$) AS (id agtype)",
        )
        .unwrap();
        let StatementKind::CypherCall(p) = k else {
            panic!("expected CypherCall, got {k:?}");
        };
        assert_eq!(p.graph, "g");
        assert_eq!(p.columns[0].name, "id");
    }

    #[test]
    fn eg116_classify_routes_create_ann_index() {
        let k = classify("CREATE INDEX ON items USING hnsw (embedding vector_l2_ops)").unwrap();
        assert!(matches!(k, StatementKind::CreateAnnIndex(_)));
    }

    #[test]
    fn eg117_classify_routes_create_hypertable() {
        let k = classify("SELECT create_hypertable('conditions', 'ts')").unwrap();
        let StatementKind::CreateHypertable(p) = k else {
            panic!("expected CreateHypertable, got {k:?}");
        };
        assert_eq!(p.table, "conditions");
        assert_eq!(p.time_column, "ts");
    }

    #[test]
    fn eg117_classify_routes_continuous_aggregate() {
        let k = classify(
            "CREATE MATERIALIZED VIEW cagg WITH (timescaledb.continuous) AS \
             SELECT time_bucket('1 hour', ts) AS b, avg(v) FROM conditions GROUP BY b",
        )
        .unwrap();
        assert!(matches!(k, StatementKind::CreateContinuousAggregate(_)));
    }

    #[test]
    fn eg119_desugar_at_at_at_to_bm25_match() {
        let out = desugar_vector_ops("SELECT id FROM docs WHERE body @@@ 'rust lang'");
        assert!(
            out.to_ascii_lowercase().contains("bm25_match"),
            "expected @@@ to desugar to bm25_match: {out}"
        );
    }

    #[test]
    fn eg119_desugar_paradedb_score_and_snippet() {
        let out = desugar_vector_ops(
            "SELECT paradedb.snippet(body) FROM docs WHERE body @@@ 'q' \
             ORDER BY paradedb.score(id) DESC",
        );
        let lower = out.to_ascii_lowercase();
        assert!(lower.contains("bm25_score"), "score not desugared: {out}");
        assert!(lower.contains("bm25_snippet"), "snippet not desugared: {out}");
        assert!(!lower.contains("paradedb."), "paradedb.* left in: {out}");
    }

    // ── CONCEPT:EG-084 — JSON operator lowering onto `Pred::JsonPath` ─────────

    fn parse_where_expr(s: &str) -> Expr {
        Parser::new(&PostgreSqlDialect {})
            .try_with_sql(s)
            .unwrap()
            .parse_expr()
            .unwrap()
    }

    #[test]
    fn eg084_lower_long_arrow_equality() {
        // `props->>'k' = 'v'` ⇒ deep equality at `$['k']`.
        let e = parse_where_expr("props->>'lang' = 'rust'");
        assert_eq!(
            json_pred_from_expr(&e).unwrap(),
            Pred::JsonPath {
                path: "$['lang']".into(),
                op: JsonPathOp::Eq {
                    value: serde_json::json!("rust")
                },
            }
        );
    }

    #[test]
    fn eg084_lower_arrow_chain_and_index() {
        // `props->'meta'->>'year' = '2024'` ⇒ `$['meta']['year']`.
        let e = parse_where_expr("props->'meta'->>'year' = '2024'");
        let Pred::JsonPath { path, op } = json_pred_from_expr(&e).unwrap() else {
            panic!("expected JsonPath");
        };
        assert_eq!(path, "$['meta']['year']");
        assert_eq!(
            op,
            JsonPathOp::Eq {
                value: serde_json::json!("2024")
            }
        );
        // Array index: `tags->0`.
        let e = parse_where_expr("tags->0 = 'a'");
        let Pred::JsonPath { path, .. } = json_pred_from_expr(&e).unwrap() else {
            panic!("expected JsonPath");
        };
        assert_eq!(path, "$[0]");
    }

    #[test]
    fn eg084_lower_hash_arrow_text_path() {
        // `props#>'{meta,lang}' = 'rust'` ⇒ `$['meta']['lang']`.
        let e = parse_where_expr("props#>'{meta,lang}' = 'rust'");
        let Pred::JsonPath { path, .. } = json_pred_from_expr(&e).unwrap() else {
            panic!("expected JsonPath");
        };
        assert_eq!(path, "$['meta']['lang']");
    }

    #[test]
    fn eg084_lower_at_arrow_containment() {
        // `props @> '{"meta":{"lang":"rust"}}'` ⇒ containment at `$`.
        let e = parse_where_expr(r#"props @> '{"meta":{"lang":"rust"}}'"#);
        assert_eq!(
            json_pred_from_expr(&e).unwrap(),
            Pred::JsonPath {
                path: "$".into(),
                op: JsonPathOp::Contains {
                    value: serde_json::json!({"meta": {"lang": "rust"}})
                },
            }
        );
    }

    #[test]
    fn eg084_lower_jsonb_path_query_existence_and_eq() {
        // Bare function ⇒ existence.
        let e = parse_where_expr("jsonb_path_query(props, '$.meta.lang')");
        assert_eq!(
            json_pred_from_expr(&e).unwrap(),
            Pred::JsonPath {
                path: "$.meta.lang".into(),
                op: JsonPathOp::Exists,
            }
        );
        // `jsonb_path_exists` ⇒ existence too.
        let e = parse_where_expr("jsonb_path_exists(props, '$.tags')");
        assert!(matches!(
            json_pred_from_expr(&e).unwrap(),
            Pred::JsonPath {
                op: JsonPathOp::Exists,
                ..
            }
        ));
        // Function `= <lit>` ⇒ equality at the jsonpath.
        let e = parse_where_expr("jsonb_path_query(props, '$.meta.lang') = 'go'");
        assert_eq!(
            json_pred_from_expr(&e).unwrap(),
            Pred::JsonPath {
                path: "$.meta.lang".into(),
                op: JsonPathOp::Eq {
                    value: serde_json::json!("go")
                },
            }
        );
    }

    #[test]
    fn eg084_bare_column_equality_is_not_json() {
        // A plain relational `col = 'v'` must NOT be lowered to JsonPath.
        let e = parse_where_expr("type = 'Doc'");
        assert!(json_pred_from_expr(&e).is_err());
    }

    #[test]
    fn eg084_mongo_match_lowering() {
        // `{ "meta.lang": "rust", "tags": {"$exists": true}, "$root": {"$contains": {...}} }`
        let filter = serde_json::json!({
            "meta.lang": "rust",
            "tags": {"$exists": true},
        });
        let mut preds = mongo_match_to_preds(&filter).unwrap();
        preds.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        assert!(preds.contains(&Pred::JsonPath {
            path: "$['meta']['lang']".into(),
            op: JsonPathOp::Eq {
                value: serde_json::json!("rust")
            },
        }));
        assert!(preds.contains(&Pred::JsonPath {
            path: "$['tags']".into(),
            op: JsonPathOp::Exists,
        }));
        // `$contains` operator form.
        let filter = serde_json::json!({"meta": {"$contains": {"lang": "go"}}});
        assert_eq!(
            mongo_match_to_preds(&filter).unwrap(),
            vec![Pred::JsonPath {
                path: "$['meta']".into(),
                op: JsonPathOp::Contains {
                    value: serde_json::json!({"lang": "go"})
                },
            }]
        );
    }
}
