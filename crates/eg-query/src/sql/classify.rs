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
    ColumnOption, CopyLegacyOption, CopyOption, CopySource, CopyTarget, CreateTable, Delete, Expr,
    FromTable, Insert, ObjectName, ObjectType, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins, Value as SqlValue, Values,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use serde_json::{Map, Value};

use crate::tables::schema::{CmpOp, ColCheck};

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
    /// `UPDATE nodes SET k = v[, …] WHERE …` — decoded SET map + a simple WHERE
    /// predicate, routed to `compare_and_set_fields` per matched node under a txn.
    UpdateNodes(UpdateNodes),
    /// `DELETE FROM nodes WHERE …` — a simple WHERE predicate, routed to
    /// `remove_node` per matched node under a txn.
    DeleteNodes(DeleteNodes),

    // ── arbitrary user-defined relational tables (CONCEPT:EG-018) ──────────────
    /// `CREATE TABLE name (col type, …) [IF NOT EXISTS]` — a new user table whose
    /// schema is recorded in the redb table catalog (NOT the graph projection).
    CreateTable(CreateTablePlan),
    /// `DROP TABLE [IF EXISTS] name` — remove a user table and all its rows.
    DropTable(DropTablePlan),
    /// `ALTER TABLE name ADD COLUMN col type` — append a column to a user table.
    AlterTable(AlterTablePlan),
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

/// A decoded literal `INSERT INTO <user_table> … VALUES …` (CONCEPT:EG-018).
#[derive(Debug, Clone, PartialEq)]
pub struct InsertTable {
    pub table: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
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

/// One or more decoded `INSERT` rows plus whether a `RETURNING` clause was present.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertNodes {
    pub rows: Vec<InsertNode>,
    pub returning: bool,
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
        Statement::Query(_)
        | Statement::Explain { .. }
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
        Statement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => classify_drop(*object_type, *if_exists, names).map(StatementKind::DropTable),
        Statement::AlterTable {
            name, operations, ..
        } => classify_alter_table(name, operations).map(StatementKind::AlterTable),
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
    })
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
    if from.is_some() {
        return Err("UPDATE … FROM is not supported (CONCEPT:KG-2.198 follow-up)".to_string());
    }
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
        return classify_insert(insert).map(StatementKind::InsertNodes);
    }
    if leaf.eq_ignore_ascii_case("edges") {
        return Err("INSERT is only supported on the `nodes` table".to_string());
    }
    classify_insert_table(insert, leaf)
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
        return classify_delete(delete).map(StatementKind::DeleteNodes);
    }
    if leaf.eq_ignore_ascii_case("edges") {
        return Err("DELETE is only supported on the `nodes` table".to_string());
    }
    if delete.using.is_some() || !target.joins.is_empty() {
        return Err("DELETE … USING/JOIN is not supported (EG-018 follow-up)".to_string());
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

/// Decode `DROP TABLE [IF EXISTS] name`. Only `TABLE` objects are handled here; any
/// other `DROP …` is rejected so it isn't silently mis-routed.
fn classify_drop(
    object_type: ObjectType,
    if_exists: bool,
    names: &[ObjectName],
) -> Result<DropTablePlan, String> {
    if object_type != ObjectType::Table {
        return Err(format!(
            "DROP {object_type} is not supported (only DROP TABLE)"
        ));
    }
    let name = match names {
        [one] => last_ident(one),
        _ => return Err("DROP TABLE supports exactly one table".to_string()),
    };
    if is_reserved_table(&name) {
        return Err(format!("cannot DROP the reserved graph table `{name}`"));
    }
    Ok(DropTablePlan { name, if_exists })
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
    if !target.joins.is_empty() {
        return Err(format!(
            "{verb} with a JOIN is not supported (CONCEPT:KG-2.198 follow-up)"
        ));
    }
    match &target.relation {
        TableFactor::Table { name, .. } => require_nodes_table(&name.to_string(), verb),
        _ => Err(format!("{verb} target must be the `nodes` table")),
    }
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
}
