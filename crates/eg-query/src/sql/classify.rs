//! Pure parse-to-classify helper for the Postgres wire shim (CONCEPT:KG-2.189).
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

use datafusion::sql::sqlparser::ast::{
    Expr, Insert, SetExpr, Statement, Value as SqlValue, Values,
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
    /// `INSERT INTO nodes (...) VALUES (...)` — a node creation, fully decoded into
    /// the id + the property object so it can be applied through a `GraphTxn`.
    InsertNode(InsertNode),
    /// `UPDATE …` — recognized but routed to the documented follow-up (not yet
    /// implemented). Carried as a distinct kind so the shim returns a precise
    /// "not yet supported" rather than a parse error.
    Update,
    /// `DELETE …` — same follow-up status as `Update`.
    Delete,
}

/// A decoded `INSERT INTO nodes (id, <prop>, …) VALUES (<id>, <v>, …)` — the only
/// write the first increment applies. `node_id` is the value of the `id` column;
/// `properties` are the remaining columns as a JSON object (the same shape the
/// AddNode write path stores as a MessagePack blob).
#[derive(Debug, Clone, PartialEq)]
pub struct InsertNode {
    pub node_id: String,
    pub properties: Map<String, Value>,
}

/// Parse `sql` (one statement) with the Postgres dialect and classify it.
///
/// Pure: no graph, no runtime, no I/O. Returns an `Err(String)` for an empty
/// batch, multiple statements (the shim handles one per call), a parse error, or
/// a write whose shape the first increment cannot route (e.g. `INSERT` into a
/// table other than `nodes`, or without an `id` column).
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
        Statement::Insert(insert) => classify_insert(insert).map(StatementKind::InsertNode),
        Statement::Update { .. } => Ok(StatementKind::Update),
        Statement::Delete(_) => Ok(StatementKind::Delete),
        other => Err(format!("unsupported statement: {other}")),
    }
}

/// Decode `INSERT INTO nodes (id, …) VALUES (…)` into a [`InsertNode`]. Only the
/// `nodes` table, a column list including `id`, and a single literal `VALUES` row
/// are accepted — anything else is an explicit error (no silent mis-route).
fn classify_insert(insert: &Insert) -> Result<InsertNode, String> {
    let table = insert.table_name.to_string();
    // ObjectName renders qualified names dotted; accept a bare or last-segment match.
    let table_leaf = table.rsplit('.').next().unwrap_or(&table);
    if !table_leaf.eq_ignore_ascii_case("nodes") {
        return Err(format!(
            "INSERT is only supported into the `nodes` table, got `{table}`"
        ));
    }
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
    let rows = match source.body.as_ref() {
        SetExpr::Values(Values { rows, .. }) => rows,
        _ => return Err("INSERT INTO nodes supports only literal VALUES rows".to_string()),
    };
    let row = match rows.as_slice() {
        [r] => r,
        [] => return Err("INSERT INTO nodes VALUES has no rows".to_string()),
        _ => {
            return Err("INSERT INTO nodes supports a single VALUES row per statement".to_string())
        }
    };
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
            node_id = Some(match val {
                Value::String(s) => s,
                Value::Number(n) => n.to_string(),
                other => return Err(format!("`id` must be a scalar, got {other}")),
            });
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

/// A literal `VALUES` cell → a JSON value. Only SQL literals are accepted (no
/// expressions/functions in the first increment), which keeps the write path a
/// pure data move with no evaluation.
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
        let StatementKind::InsertNode(n) = k else {
            panic!("expected InsertNode, got {k:?}");
        };
        assert_eq!(n.node_id, "n1");
        assert_eq!(n.properties.get("type").unwrap(), "Agent");
        assert_eq!(n.properties.get("rank").unwrap(), &Value::Number(7.into()));
        assert_eq!(n.properties.get("active").unwrap(), &Value::Bool(true));
    }

    #[test]
    fn insert_negative_number() {
        let k = classify("INSERT INTO nodes (id, delta) VALUES ('n1', -3)").unwrap();
        let StatementKind::InsertNode(n) = k else {
            panic!("expected InsertNode");
        };
        assert_eq!(
            n.properties.get("delta").unwrap(),
            &Value::Number((-3).into())
        );
    }

    #[test]
    fn update_and_delete_are_their_own_kinds() {
        assert_eq!(
            classify("UPDATE nodes SET rank = 2 WHERE id = 'n1'").unwrap(),
            StatementKind::Update
        );
        assert_eq!(
            classify("DELETE FROM nodes WHERE id = 'n1'").unwrap(),
            StatementKind::Delete
        );
    }

    #[test]
    fn insert_into_other_table_rejected() {
        let e = classify("INSERT INTO edges (src, dst) VALUES ('a', 'b')").unwrap_err();
        assert!(e.contains("only supported into the `nodes` table"), "{e}");
    }

    #[test]
    fn insert_without_id_rejected() {
        let e = classify("INSERT INTO nodes (type) VALUES ('Agent')").unwrap_err();
        assert!(e.contains("must set the `id` column"), "{e}");
    }

    #[test]
    fn parse_error_surfaces() {
        assert!(classify("SELEKT 1").is_err());
        assert!(classify("").is_err());
        assert!(classify("SELECT 1; SELECT 2").is_err());
    }
}
