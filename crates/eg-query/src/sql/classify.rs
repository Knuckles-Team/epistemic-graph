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
    Assignment, AssignmentTarget, BinaryOperator, Delete, Expr, FromTable, Insert, ObjectName,
    SelectItem, SetExpr, Statement, TableFactor, Value as SqlValue, Values,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use serde_json::{Map, Value};

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
}

/// A simple single-column equality predicate, the only WHERE shape the wire DML
/// path resolves. `WHERE id = '…'` is the fast path (one node by id); any other
/// `WHERE <prop> = <literal>` selects every node whose `<prop>` equals the literal.
#[derive(Debug, Clone, PartialEq)]
pub enum WhereEq {
    /// `WHERE id = <value>` — addresses exactly one node by its id.
    Id(String),
    /// `WHERE <column> = <value>` — addresses every node whose property
    /// `<column>` currently equals `<value>`.
    Property { column: String, value: Value },
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
    let stmts =
        Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|e| format!("parse error: {e}"))?;
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
        Statement::Insert(insert) => classify_insert(insert).map(StatementKind::InsertNodes),
        Statement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
        } => classify_update(
            table,
            assignments,
            from.as_ref(),
            selection.as_ref(),
            returning,
        )
        .map(StatementKind::UpdateNodes),
        Statement::Delete(delete) => classify_delete(delete).map(StatementKind::DeleteNodes),
        other => Err(format!("unsupported statement: {other}")),
    }
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

/// Decode a WHERE clause into the single simple-equality predicate the wire DML
/// path can resolve. A missing WHERE is rejected (no unscoped mass UPDATE/DELETE).
fn decode_where(selection: Option<&Expr>, verb: &str) -> Result<WhereEq, String> {
    let expr = selection.ok_or_else(|| {
        format!(
            "{verb} nodes requires a WHERE clause (an unscoped {verb} is refused; \
             use `WHERE id = '…'` or `WHERE <prop> = <literal>`)"
        )
    })?;
    match expr {
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
            let column = ident_column(left)?;
            let value = expr_to_json(right)?;
            if column.eq_ignore_ascii_case("id") {
                Ok(WhereEq::Id(scalar_id(value)?))
            } else {
                Ok(WhereEq::Property { column, value })
            }
        }
        other => Err(format!(
            "{verb} nodes supports only a single `<column> = <literal>` WHERE \
             (complex predicates are a CONCEPT:KG-2.198 follow-up), got `{other}`"
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
        assert_eq!(
            u.selector,
            WhereEq::Property {
                column: "type".to_string(),
                value: Value::String("Tool".to_string()),
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
    fn update_complex_where_rejected() {
        let e =
            classify("UPDATE nodes SET rank = 1 WHERE rank > 2 AND type = 'Agent'").unwrap_err();
        assert!(e.contains("single") || e.contains("follow-up"), "{e}");
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
        assert_eq!(
            d.selector,
            WhereEq::Property {
                column: "type".to_string(),
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
}
