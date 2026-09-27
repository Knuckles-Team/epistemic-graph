//! Bounded, read-only external SQL source and column reader.

use super::ForeignSource;
use crate::rowset::RowSet;

#[cfg(feature = "federation-sql")]
const MAX_FEDERATED_SQL_BYTES: usize = 1024 * 1024;
#[cfg(feature = "federation-sql")]
const MAX_FEDERATED_SQL_DSN_BYTES: usize = 8 * 1024;
#[cfg(feature = "federation-sql")]
const MAX_FEDERATED_SQL_ROWS: usize = 250_000;
#[cfg(feature = "federation-sql")]
const MAX_FEDERATED_SQL_FIELD_BYTES: usize = 1024;
#[cfg(feature = "federation-sql")]
const MAX_FEDERATED_SQL_ID_BYTES: usize = 64 * 1024;
#[cfg(feature = "federation-sql")]
const FEDERATED_SQL_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
#[cfg(feature = "federation-sql")]
const FEDERATED_SQL_QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

// ── kind (c) impl: external relational-SQL (CONCEPT:EG-KG.query.feature, feature `federation-sql`) ─

#[cfg(feature = "federation-sql")]
#[derive(Clone, Copy)]
pub(crate) enum SqlDialect {
    Postgres,
    MySql,
}

/// SQLx 0.9 deliberately requires an explicit audit marker for runtime SQL. This
/// parser is that audit boundary: exactly one query is accepted, including every
/// nested statement, and locking/`SELECT INTO` write-shaped queries are rejected.
/// The database read-only transaction below remains the authoritative second layer.
#[cfg(feature = "federation-sql")]
pub(crate) fn validate_federated_sql(query: &str, dialect: SqlDialect) -> Result<(), String> {
    use core::ops::ControlFlow;
    use sqlparser::ast::{Query, Select, Statement, Visit, Visitor};
    use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect};
    use sqlparser::parser::Parser;

    if query.is_empty() || query.len() > MAX_FEDERATED_SQL_BYTES || query.contains('\0') {
        return Err("federation: invalid SQL query size or encoding".to_string());
    }
    let statements = match dialect {
        SqlDialect::Postgres => Parser::parse_sql(&PostgreSqlDialect {}, query),
        SqlDialect::MySql => Parser::parse_sql(&MySqlDialect {}, query),
    }
    .map_err(|_| "federation: SQL query did not parse".to_string())?;
    if statements.len() != 1 || !matches!(statements.first(), Some(Statement::Query(_))) {
        return Err("federation: SQL source requires exactly one read query".to_string());
    }

    struct ReadOnlyVisitor;
    impl Visitor for ReadOnlyVisitor {
        type Break = ();

        fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
            if matches!(statement, Statement::Query(_)) {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        }

        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
            if query.locks.is_empty() {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        }

        fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<Self::Break> {
            if select.into.is_none() {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        }
    }

    let mut visitor = ReadOnlyVisitor;
    if statements[0].visit(&mut visitor).is_break() {
        return Err("federation: SQL query is not read-only".to_string());
    }
    Ok(())
}

/// Reads rows from an EXTERNAL relational DB (Postgres/MySQL) over a pure-Rust/rustls
/// `sqlx` client. The statement must parse as one read query, then executes inside a
/// database-enforced read-only transaction with time and cardinality bounds. The DSN scheme picks the dialect
/// (`postgres://`/`postgresql://` ⇒ Postgres, `mysql://` ⇒ MySQL); each row's `id_field`
/// column becomes the row id and the optional `score_field` becomes the row score.
///
/// `fetch()` is SYNC (the executor runs it on the blocking pool, exactly like the SQL /
/// vector legs) but `sqlx` is async, so it spins a small current-thread tokio runtime to
/// drive the connect+query to completion. A per-call connection prevents session state
/// from crossing requests.
#[cfg(feature = "federation-sql")]
pub struct SqlSource<'a> {
    pub(super) dsn: &'a str,
    pub(super) query: &'a str,
    pub(super) id_field: &'a str,
    pub(super) score_field: Option<&'a str>,
}

#[cfg(feature = "federation-sql")]
impl ForeignSource for SqlSource<'_> {
    fn fetch(&self) -> Result<RowSet, String> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("federation: build tokio runtime: {e}"))?;
        rt.block_on(self.fetch_async())
    }
}

#[cfg(feature = "federation-sql")]
impl SqlSource<'_> {
    async fn fetch_async(&self) -> Result<RowSet, String> {
        if self.dsn.is_empty()
            || self.dsn.len() > MAX_FEDERATED_SQL_DSN_BYTES
            || self.dsn.contains('\0')
        {
            return Err("federation: invalid SQL connection configuration".to_string());
        }
        // The destination gate runs after the local checks (scheme, statement shape) and
        // before any connection, so a refused query never reaches the network either way.
        let scheme = self.dsn.split(':').next().unwrap_or("");
        match scheme {
            "postgres" | "postgresql" => {
                validate_federated_sql(self.query, SqlDialect::Postgres)?;
                crate::federation_ssrf::check_sql_dsn(self.dsn)?;
                self.fetch_postgres().await
            }
            "mysql" | "mariadb" => {
                validate_federated_sql(self.query, SqlDialect::MySql)?;
                crate::federation_ssrf::check_sql_dsn(self.dsn)?;
                self.fetch_mysql().await
            }
            _ => Err(
                "federation: unsupported SQL connection scheme (expected postgres or mysql)"
                    .to_string(),
            ),
        }
    }

    async fn fetch_postgres(&self) -> Result<RowSet, String> {
        use futures_util::TryStreamExt;
        use sqlx::Connection;
        self.validate_projection_fields()?;
        let mut conn = tokio::time::timeout(
            FEDERATED_SQL_CONNECT_TIMEOUT,
            sqlx::postgres::PgConnection::connect(self.dsn),
        )
        .await
        .map_err(|_| "federation: postgres connection timed out".to_string())?
        .map_err(|_| "federation: postgres connection failed".to_string())?;
        let mut tx = conn
            .begin()
            .await
            .map_err(|_| "federation: postgres transaction failed".to_string())?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(|_| "federation: postgres read-only transaction unavailable".to_string())?;
        let out = tokio::time::timeout(FEDERATED_SQL_QUERY_TIMEOUT, async {
            let mut rows = sqlx::query(sqlx::AssertSqlSafe(self.query.to_owned())).fetch(&mut *tx);
            let mut out = Vec::new();
            while let Some(row) = rows
                .try_next()
                .await
                .map_err(|_| "federation: postgres query failed".to_string())?
            {
                if out.len() >= MAX_FEDERATED_SQL_ROWS {
                    return Err("federation: postgres result exceeds row limit".to_string());
                }
                out.push(self.project_pg_row(&row)?);
            }
            Ok::<_, String>(out)
        })
        .await
        .map_err(|_| "federation: postgres query timed out".to_string())??;
        tx.rollback()
            .await
            .map_err(|_| "federation: postgres read-only transaction cleanup failed".to_string())?;
        Ok(RowSet::from_rows(out))
    }

    async fn fetch_mysql(&self) -> Result<RowSet, String> {
        use futures_util::TryStreamExt;
        use sqlx::Connection;
        self.validate_projection_fields()?;
        let mut conn = tokio::time::timeout(
            FEDERATED_SQL_CONNECT_TIMEOUT,
            sqlx::mysql::MySqlConnection::connect(self.dsn),
        )
        .await
        .map_err(|_| "federation: mysql connection timed out".to_string())?
        .map_err(|_| "federation: mysql connection failed".to_string())?;
        // MySQL applies this setting to the next transaction; the connection is
        // request-scoped and discarded immediately afterward.
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut conn)
            .await
            .map_err(|_| "federation: mysql read-only transaction unavailable".to_string())?;
        let mut tx = conn
            .begin()
            .await
            .map_err(|_| "federation: mysql transaction failed".to_string())?;
        let out = tokio::time::timeout(FEDERATED_SQL_QUERY_TIMEOUT, async {
            let mut rows = sqlx::query(sqlx::AssertSqlSafe(self.query.to_owned())).fetch(&mut *tx);
            let mut out = Vec::new();
            while let Some(row) = rows
                .try_next()
                .await
                .map_err(|_| "federation: mysql query failed".to_string())?
            {
                if out.len() >= MAX_FEDERATED_SQL_ROWS {
                    return Err("federation: mysql result exceeds row limit".to_string());
                }
                out.push(self.project_my_row(&row)?);
            }
            Ok::<_, String>(out)
        })
        .await
        .map_err(|_| "federation: mysql query timed out".to_string())??;
        tx.rollback()
            .await
            .map_err(|_| "federation: mysql read-only transaction cleanup failed".to_string())?;
        Ok(RowSet::from_rows(out))
    }

    fn validate_projection_fields(&self) -> Result<(), String> {
        for (name, value) in [
            ("id_field", Some(self.id_field)),
            ("score_field", self.score_field),
        ] {
            if let Some(value) = value {
                if value.is_empty()
                    || value.len() > MAX_FEDERATED_SQL_FIELD_BYTES
                    || value.contains('\0')
                {
                    return Err(format!("federation: invalid {name}"));
                }
            }
        }
        Ok(())
    }

    fn project_pg_row(&self, row: &sqlx::postgres::PgRow) -> Result<(String, Option<f32>), String> {
        use sqlx::Row;
        let id = pg_col_to_id(row, self.id_field)?;
        if id.len() > MAX_FEDERATED_SQL_ID_BYTES {
            return Err("federation: postgres id exceeds size limit".to_string());
        }
        // Read the score as f64 (float8/numeric-via-double) or f32 (float4) — a NULL or a
        // non-numeric column yields no score rather than erroring (the score is optional).
        let score = self.score_field.and_then(|sf| {
            row.try_get::<f64, _>(sf)
                .map(|v| v as f32)
                .or_else(|_| row.try_get::<f32, _>(sf))
                .ok()
        });
        Ok((id, score))
    }

    fn project_my_row(&self, row: &sqlx::mysql::MySqlRow) -> Result<(String, Option<f32>), String> {
        use sqlx::Row;
        let id = my_col_to_id(row, self.id_field)?;
        if id.len() > MAX_FEDERATED_SQL_ID_BYTES {
            return Err("federation: mysql id exceeds size limit".to_string());
        }
        let score = self.score_field.and_then(|sf| {
            row.try_get::<f64, _>(sf)
                .map(|v| v as f32)
                .or_else(|_| row.try_get::<f32, _>(sf))
                .ok()
        });
        Ok((id, score))
    }
}

/// CONCEPT:EG-KG.query.obda-predicate-pushdown — run a rendered READ-ONLY `SELECT` against an
/// external Postgres/MySQL database and return each row as `column → lexical-string`. The
/// COLUMN-carrying sibling of [`SqlSource::fetch`] (which yields the id+score `RowSet`): the
/// OBDA external-source seam needs full columns to fill R2RML templates. Reuses the SAME
/// SSRF-validated DSN handling, `validate_federated_sql` statement check, read-only
/// transaction, connect/query timeouts, and row cap. The caller (`SqlObdaSource`) casts every
/// selected column to text, so each value decodes as `Option<String>`; a NULL column is omitted
/// (so an R2RML template/column over it correctly yields no triple). Sync — it spins a small
/// current-thread tokio runtime, exactly like [`SqlSource::fetch`].
#[cfg(feature = "federation-sql")]
pub fn fetch_sql_columns(
    dsn: &str,
    sql: &str,
) -> Result<Vec<std::collections::HashMap<String, String>>, String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("federation: build tokio runtime: {e}"))?;
    rt.block_on(fetch_sql_columns_async(dsn, sql))
}

#[cfg(feature = "federation-sql")]
async fn fetch_sql_columns_async(
    dsn: &str,
    sql: &str,
) -> Result<Vec<std::collections::HashMap<String, String>>, String> {
    if dsn.is_empty() || dsn.len() > MAX_FEDERATED_SQL_DSN_BYTES || dsn.contains('\0') {
        return Err("federation: invalid SQL connection configuration".to_string());
    }
    match dsn.split(':').next().unwrap_or("") {
        "postgres" | "postgresql" => {
            validate_federated_sql(sql, SqlDialect::Postgres)?;
            crate::federation_ssrf::check_sql_dsn(dsn)?;
            fetch_pg_columns(dsn, sql).await
        }
        "mysql" | "mariadb" => {
            validate_federated_sql(sql, SqlDialect::MySql)?;
            crate::federation_ssrf::check_sql_dsn(dsn)?;
            fetch_my_columns(dsn, sql).await
        }
        _ => Err(
            "federation: unsupported SQL connection scheme (expected postgres or mysql)"
                .to_string(),
        ),
    }
}

#[cfg(feature = "federation-sql")]
async fn fetch_pg_columns(
    dsn: &str,
    sql: &str,
) -> Result<Vec<std::collections::HashMap<String, String>>, String> {
    use futures_util::TryStreamExt;
    use sqlx::{Column, Connection, Row};
    let mut conn = tokio::time::timeout(
        FEDERATED_SQL_CONNECT_TIMEOUT,
        sqlx::postgres::PgConnection::connect(dsn),
    )
    .await
    .map_err(|_| "federation: postgres connection timed out".to_string())?
    .map_err(|_| "federation: postgres connection failed".to_string())?;
    let mut tx = conn
        .begin()
        .await
        .map_err(|_| "federation: postgres transaction failed".to_string())?;
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *tx)
        .await
        .map_err(|_| "federation: postgres read-only transaction unavailable".to_string())?;
    let out = tokio::time::timeout(FEDERATED_SQL_QUERY_TIMEOUT, async {
        let mut rows = sqlx::query(sqlx::AssertSqlSafe(sql.to_owned())).fetch(&mut *tx);
        let mut out = Vec::new();
        while let Some(row) = rows
            .try_next()
            .await
            .map_err(|_| "federation: postgres query failed".to_string())?
        {
            if out.len() >= MAX_FEDERATED_SQL_ROWS {
                return Err("federation: postgres result exceeds row limit".to_string());
            }
            let map = row
                .columns()
                .iter()
                .enumerate()
                .filter_map(|(i, col)| {
                    row.try_get::<Option<String>, _>(i)
                        .ok()
                        .flatten()
                        .map(|v| (col.name().to_string(), v))
                })
                .collect::<std::collections::HashMap<String, String>>();
            out.push(map);
        }
        Ok::<_, String>(out)
    })
    .await
    .map_err(|_| "federation: postgres query timed out".to_string())??;
    tx.rollback()
        .await
        .map_err(|_| "federation: postgres read-only transaction cleanup failed".to_string())?;
    Ok(out)
}

#[cfg(feature = "federation-sql")]
async fn fetch_my_columns(
    dsn: &str,
    sql: &str,
) -> Result<Vec<std::collections::HashMap<String, String>>, String> {
    use futures_util::TryStreamExt;
    use sqlx::{Column, Connection, Row};
    let mut conn = tokio::time::timeout(
        FEDERATED_SQL_CONNECT_TIMEOUT,
        sqlx::mysql::MySqlConnection::connect(dsn),
    )
    .await
    .map_err(|_| "federation: mysql connection timed out".to_string())?
    .map_err(|_| "federation: mysql connection failed".to_string())?;
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut conn)
        .await
        .map_err(|_| "federation: mysql read-only transaction unavailable".to_string())?;
    let mut tx = conn
        .begin()
        .await
        .map_err(|_| "federation: mysql transaction failed".to_string())?;
    let out = tokio::time::timeout(FEDERATED_SQL_QUERY_TIMEOUT, async {
        let mut rows = sqlx::query(sqlx::AssertSqlSafe(sql.to_owned())).fetch(&mut *tx);
        let mut out = Vec::new();
        while let Some(row) = rows
            .try_next()
            .await
            .map_err(|_| "federation: mysql query failed".to_string())?
        {
            if out.len() >= MAX_FEDERATED_SQL_ROWS {
                return Err("federation: mysql result exceeds row limit".to_string());
            }
            let map = row
                .columns()
                .iter()
                .enumerate()
                .filter_map(|(i, col)| {
                    row.try_get::<Option<String>, _>(i)
                        .ok()
                        .flatten()
                        .map(|v| (col.name().to_string(), v))
                })
                .collect::<std::collections::HashMap<String, String>>();
            out.push(map);
        }
        Ok::<_, String>(out)
    })
    .await
    .map_err(|_| "federation: mysql query timed out".to_string())??;
    tx.rollback()
        .await
        .map_err(|_| "federation: mysql read-only transaction cleanup failed".to_string())?;
    Ok(out)
}

/// Read the `id_field` column of a Postgres row as a String id, trying the common id
/// SQL types in order (text, then integer, then float). A column that decodes as none of
/// these errors clearly (rather than silently dropping the row).
#[cfg(feature = "federation-sql")]
fn pg_col_to_id(row: &sqlx::postgres::PgRow, col: &str) -> Result<String, String> {
    use sqlx::Row;
    if let Ok(s) = row.try_get::<String, _>(col) {
        return Ok(s);
    }
    if let Ok(n) = row.try_get::<i64, _>(col) {
        return Ok(n.to_string());
    }
    if let Ok(n) = row.try_get::<i32, _>(col) {
        return Ok(n.to_string());
    }
    if let Ok(f) = row.try_get::<f64, _>(col) {
        return Ok(f.to_string());
    }
    Err(format!(
        "federation: id column '{col}' is not a string/int/float (cast it to text in the query)"
    ))
}

/// MySQL counterpart of [`pg_col_to_id`].
#[cfg(feature = "federation-sql")]
fn my_col_to_id(row: &sqlx::mysql::MySqlRow, col: &str) -> Result<String, String> {
    use sqlx::Row;
    if let Ok(s) = row.try_get::<String, _>(col) {
        return Ok(s);
    }
    if let Ok(n) = row.try_get::<i64, _>(col) {
        return Ok(n.to_string());
    }
    if let Ok(n) = row.try_get::<i32, _>(col) {
        return Ok(n.to_string());
    }
    if let Ok(f) = row.try_get::<f64, _>(col) {
        return Ok(f.to_string());
    }
    Err(format!(
        "federation: id column '{col}' is not a string/int/float (cast it to text in the query)"
    ))
}
