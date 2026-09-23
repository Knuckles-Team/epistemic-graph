//! The read-only gate of the graph-free SQL path (EH-387).
//!
//! [`crate::exec_sql_over_tables`] runs caller SQL through a fresh DataFusion
//! `SessionContext`. That context would happily execute DDL, DML, `COPY` and, above
//! all, DataFusion's `CREATE EXTERNAL TABLE` (a server-side file read). Two surfaces
//! hand it caller SQL: the observability log search and the Decide `DecisionLog`
//! query view (EH-066). Rather than trusting each caller to pre-check, the path itself
//! refuses anything that does not classify as ONE read statement, using the same
//! classifier (`classify`) the SQL surface and the Decide view use.

use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;

use super::classify::{classify, desugar_vector_ops, StatementKind};

/// Refuse `sql` unless it is exactly one read statement (`SELECT`/`WITH`/`SHOW`/
/// `EXPLAIN <read>`). The input is classified AFTER the engine's operator desugaring
/// (`<->`, `<=>`, `@@@`, …), so it sees exactly what the executor would plan. An
/// `EXPLAIN` is judged by the statement it explains: `EXPLAIN ANALYZE` EXECUTES its
/// plan, so `EXPLAIN ANALYZE COPY … TO '<file>'` would otherwise write a server file.
/// Every refusal carries the typed `READ_ONLY_SQL` prefix.
pub fn require_single_read(sql: &str) -> Result<(), String> {
    let sql = desugar_vector_ops(sql);
    match classify(&sql) {
        Ok(StatementKind::Read) => match explained_statement(&sql) {
            Some(inner) => require_single_read(&inner),
            None => Ok(()),
        },
        Ok(_) => {
            Err("READ_ONLY_SQL: this SQL surface accepts a single read statement only".to_string())
        }
        Err(reason) => Err(format!(
            "READ_ONLY_SQL: not a single read statement ({reason})"
        )),
    }
}

/// The statement an `EXPLAIN` wraps, re-serialized; `None` for anything else.
fn explained_statement(sql: &str) -> Option<String> {
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    match stmts.as_slice() {
        [Statement::Explain { statement, .. }] => Some(statement.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_single_read_statement_passes() {
        for refused in [
            "CREATE TABLE t (a INT)",
            "DROP TABLE logs",
            "INSERT INTO logs (a) VALUES (1)",
            "UPDATE logs SET a = 1",
            "DELETE FROM logs",
            "COPY logs TO '/tmp/out.csv'",
            "CREATE EXTERNAL TABLE secrets STORED AS CSV LOCATION '/etc/passwd'",
            "SELECT 1; SELECT 2",
            "SELECT 1; DROP TABLE logs",
            "EXPLAIN ANALYZE COPY logs TO '/tmp/out.csv'",
            "EXPLAIN ANALYZE DELETE FROM logs",
            "",
        ] {
            let err = require_single_read(refused).expect_err(refused);
            assert!(err.starts_with("READ_ONLY_SQL"), "{refused}: {err}");
        }
        for read in [
            "SELECT severity, count(*) FROM logs GROUP BY severity",
            "WITH x AS (SELECT 1 AS a) SELECT a FROM x",
            "SELECT id FROM docs WHERE body @@@ 'rust'",
            "EXPLAIN SELECT 1",
        ] {
            require_single_read(read).unwrap_or_else(|e| panic!("{read}: {e}"));
        }
    }

    /// The dangerous case at the path itself: a `CREATE EXTERNAL TABLE` pointing at a
    /// server file is refused before any DataFusion context exists.
    #[test]
    fn the_graph_free_path_refuses_an_external_table_file_read() {
        let err = crate::exec_sql_over_tables(
            Vec::new(),
            "CREATE EXTERNAL TABLE leak STORED AS CSV LOCATION '/etc/hostname'",
        )
        .expect_err("a server-side file read must not run");
        assert!(err.starts_with("READ_ONLY_SQL"), "{err}");
        let ok = crate::exec_sql_over_tables(Vec::new(), "SELECT 1 AS one").expect("a read runs");
        assert_eq!(ok.rows.len(), 1);
    }
}
