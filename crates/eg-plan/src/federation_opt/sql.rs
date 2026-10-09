//! SQL source capabilities (EH-563 FO-02): batched key lookups and LIMIT pushdown for a
//! `ForeignSourceSpec::Sql`. The pushed statement is rendered here and executed through the
//! SAME validated, read-only, SSRF-gated `SqlSource` path as the caller's own statement.
//!
//! * keys: `SELECT * FROM (<query>) AS eg_fed WHERE eg_fed.<id> IN ('k1', …)` — the
//!   quoted literal takes the id column's type on the server, so a primary-key index
//!   serves it; a MySQL key containing `\` is not pushed (its meaning depends on
//!   `NO_BACKSLASH_ESCAPES`). The rendered statement must re-parse into exactly that shape.
//! * limit: set on the statement's own `LIMIT` clause through the parsed AST, so an
//!   `ORDER BY` still decides which rows come first; skipped for `FETCH`, `LIMIT … BY`,
//!   `OFFSET`-comma and non-literal limits.

use eg_types::wire::ForeignSourceSpec;

use super::capability::{KeyLookup, LimitPushdown, RemoteRequest, SourceCapabilities};
use super::remote::{Identity, RemoteFetch};
use crate::rowset::RowSet;
use crate::sql_text::{quote_identifier, render_literal, validate_identifier, SqlDialect};

/// Keys per `IN` list (well under every driver's bind/statement limits).
const MAX_SQL_KEYS: usize = 1000;
/// The alias the key-lookup wrapper gives the caller's statement.
const WRAP_ALIAS: &str = "eg_fed";

/// The borrowed fields of a `Sql` spec.
pub(crate) struct SqlSpec<'a> {
    pub(crate) dsn: &'a str,
    pub(crate) query: &'a str,
    pub(crate) id_field: &'a str,
    pub(crate) score_field: Option<&'a str>,
}

/// A SQL foreign source the optimizer can push keys and limits into.
pub(crate) struct SqlRemote<'a> {
    spec: SqlSpec<'a>,
    identity: Identity,
    dialect: Option<SqlDialect>,
}

impl<'a> SqlRemote<'a> {
    pub(crate) fn new(spec: SqlSpec<'a>, identity: Identity) -> Self {
        let dialect = SqlDialect::from_dsn(spec.dsn);
        Self {
            spec,
            identity,
            dialect,
        }
    }

    /// Run `query` through the validated `SqlSource` path.
    fn run(&self, query: String) -> Result<RowSet, String> {
        let spec = ForeignSourceSpec::Sql {
            dsn: self.spec.dsn.to_string(),
            query,
            id_field: self.spec.id_field.to_string(),
            score_field: self.spec.score_field.map(str::to_string),
            columns: Vec::new(),
        };
        let source = crate::federation::source_for(&spec);
        source.fetch()
    }

    /// The statement a request pushes, or `None` when nothing can be pushed (the caller's
    /// own statement then runs unchanged).
    fn pushed_statement(&self, request: &RemoteRequest) -> Option<String> {
        let dialect = self.dialect?;
        if !request.keys.is_empty() {
            return render_key_lookup(self.spec.query, self.spec.id_field, &request.keys, dialect)
                .ok();
        }
        request
            .limit
            .and_then(|k| limit_statement(self.spec.query, k, dialect))
    }
}

impl RemoteFetch for SqlRemote<'_> {
    fn parallel_safe(&self) -> Option<&(dyn RemoteFetch + Sync)> {
        Some(self)
    }
    fn capabilities(&self) -> SourceCapabilities {
        let Some(dialect) = self.dialect else {
            return SourceCapabilities::fetch_only();
        };
        let key_lookup = if cfg!(feature = "federation-sql")
            && validate_identifier(self.spec.id_field).is_ok()
        {
            KeyLookup::Batched {
                max_keys: MAX_SQL_KEYS,
            }
        } else {
            KeyLookup::Unsupported
        };
        let limit = match limit_statement(self.spec.query, 1, dialect) {
            Some(_) => LimitPushdown::Native,
            None => LimitPushdown::Unsupported,
        };
        SourceCapabilities::single_full_fetch(key_lookup, limit)
    }

    fn identity(&self) -> &Identity {
        &self.identity
    }

    fn fetch(&self, request: &RemoteRequest) -> Result<RowSet, String> {
        match self.pushed_statement(request) {
            Some(statement) => self.run(statement),
            None if !request.keys.is_empty() => {
                Err("federation: SQL key lookup could not be rendered".to_string())
            }
            None => self.run(self.spec.query.to_string()),
        }
    }

    fn key_expressible(&self, key: &str) -> bool {
        let mysql_backslash = self.dialect == Some(SqlDialect::MySql) && key.contains('\\');
        !(key.contains('\0') || mysql_backslash)
    }
}

/// The caller's statement without trailing whitespace/semicolons (it becomes a subquery).
fn statement_body(query: &str) -> &str {
    query.trim_end().trim_end_matches(';').trim_end()
}

/// `SELECT * FROM (<query>) AS eg_fed WHERE eg_fed.<id> IN (<keys>)`, re-parsed to prove it
/// kept exactly that shape.
pub(crate) fn render_key_lookup(
    query: &str,
    id_field: &str,
    keys: &[String],
    dialect: SqlDialect,
) -> Result<String, String> {
    let id = quote_identifier(id_field, dialect)?;
    let members = keys
        .iter()
        .map(|k| render_literal(k, false, dialect))
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    let statement = format!(
        "SELECT * FROM ({}) AS {WRAP_ALIAS} WHERE {WRAP_ALIAS}.{id} IN ({members})",
        statement_body(query)
    );
    check_key_lookup_shape(&statement, keys.len(), dialect)?;
    Ok(statement)
}

#[cfg(feature = "federation-sql")]
fn parse_one(statement: &str, dialect: SqlDialect) -> Option<sqlparser::ast::Statement> {
    use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect};
    use sqlparser::parser::Parser;
    let mut parsed = match dialect {
        SqlDialect::Postgres => Parser::parse_sql(&PostgreSqlDialect {}, statement),
        SqlDialect::MySql => Parser::parse_sql(&MySqlDialect {}, statement),
    }
    .ok()?;
    (parsed.len() == 1).then(|| parsed.remove(0))
}

/// The wrapper must re-parse as ONE query whose outer `WHERE` is an `IN` list of exactly
/// `keys` members over one derived table — the caller's statement cannot have absorbed or
/// commented out the key predicate.
#[cfg(feature = "federation-sql")]
fn check_key_lookup_shape(statement: &str, keys: usize, dialect: SqlDialect) -> Result<(), String> {
    use sqlparser::ast::{Expr, SetExpr, Statement, TableFactor};
    let refused = || "federation: SQL key lookup did not keep its shape".to_string();
    let Some(Statement::Query(query)) = parse_one(statement, dialect) else {
        return Err(refused());
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(refused());
    };
    let derived = matches!(
        select.from.as_slice(),
        [twj] if twj.joins.is_empty() && matches!(twj.relation, TableFactor::Derived { .. })
    );
    let in_list = matches!(
        &select.selection,
        Some(Expr::InList { list, negated: false, .. }) if list.len() == keys
    );
    (derived && in_list).then_some(()).ok_or_else(refused)
}

#[cfg(not(feature = "federation-sql"))]
fn check_key_lookup_shape(
    _statement: &str,
    _keys: usize,
    _dialect: SqlDialect,
) -> Result<(), String> {
    Err("federation: SQL key lookup needs the federation-sql feature".to_string())
}

/// The caller's statement with its own `LIMIT` set to `min(existing, k)`, or `None` when the
/// statement's shape makes that unsafe to express.
#[cfg(feature = "federation-sql")]
pub(crate) fn limit_statement(query: &str, k: usize, dialect: SqlDialect) -> Option<String> {
    use sqlparser::ast::{Expr, LimitClause, Statement, Value};
    let Some(Statement::Query(mut q)) = parse_one(statement_body(query), dialect) else {
        return None;
    };
    if q.fetch.is_some() || !q.pipe_operators.is_empty() {
        return None;
    }
    let existing = match &q.limit_clause {
        None => None,
        Some(LimitClause::LimitOffset {
            limit: None,
            limit_by,
            ..
        }) if limit_by.is_empty() => None,
        Some(LimitClause::LimitOffset {
            limit: Some(n),
            limit_by,
            ..
        }) if limit_by.is_empty() => Some(literal_limit(n)?),
        Some(_) => return None,
    };
    let limit = existing.map_or(k, |n| n.min(k));
    let offset = match q.limit_clause.take() {
        Some(LimitClause::LimitOffset { offset, .. }) => offset,
        _ => None,
    };
    q.limit_clause = Some(LimitClause::LimitOffset {
        limit: Some(Expr::value(Value::Number(limit.to_string(), false))),
        offset,
        limit_by: Vec::new(),
    });
    Some(Statement::Query(q).to_string())
}

/// A literal non-negative integer `LIMIT` value; `None` for anything else.
#[cfg(feature = "federation-sql")]
fn literal_limit(expr: &sqlparser::ast::Expr) -> Option<usize> {
    use sqlparser::ast::{Expr, Value};
    match expr {
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => n.parse().ok(),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(not(feature = "federation-sql"))]
pub(crate) fn limit_statement(_query: &str, _k: usize, _dialect: SqlDialect) -> Option<String> {
    None
}

#[cfg(all(test, feature = "federation-sql"))]
mod tests {
    use super::*;

    fn keys(ks: &[&str]) -> Vec<String> {
        ks.iter().map(|k| k.to_string()).collect()
    }

    #[test]
    fn key_lookups_wrap_the_statement_and_escape_every_key() {
        let sql = render_key_lookup(
            "SELECT id, w FROM papers ORDER BY w DESC;",
            "id",
            &keys(&["a", "O'b"]),
            SqlDialect::Postgres,
        )
        .unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM (SELECT id, w FROM papers ORDER BY w DESC) AS eg_fed \
             WHERE eg_fed.\"id\" IN ('a', 'O''b')"
        );
        let hostile = render_key_lookup(
            "SELECT id FROM t",
            "id",
            &keys(&["x') OR 1=1 --"]),
            SqlDialect::MySql,
        )
        .unwrap();
        assert!(hostile.ends_with("IN ('x'') OR 1=1 --')"), "{hostile}");
    }

    #[test]
    fn a_statement_that_swallows_the_key_predicate_is_refused() {
        for query in [
            "SELECT id FROM t --",
            "SELECT id FROM t /*",
            "SELECT id FROM t) AS x --",
        ] {
            assert!(
                render_key_lookup(query, "id", &keys(&["a"]), SqlDialect::Postgres).is_err(),
                "{query}"
            );
        }
        assert!(
            render_key_lookup("SELECT 1", "id; DROP", &keys(&["a"]), SqlDialect::Postgres).is_err()
        );
    }

    #[test]
    fn the_limit_goes_into_the_statements_own_limit_clause() {
        let pg = SqlDialect::Postgres;
        let ordered = limit_statement("SELECT id FROM t ORDER BY w DESC", 5, pg).unwrap();
        assert!(ordered.ends_with("ORDER BY w DESC LIMIT 5"), "{ordered}");
        let tighter = limit_statement("SELECT id FROM t LIMIT 3", 5, pg).unwrap();
        assert!(tighter.ends_with("LIMIT 3"), "{tighter}");
        let looser = limit_statement("SELECT id FROM t LIMIT 50 OFFSET 20", 5, pg).unwrap();
        assert!(
            looser.contains("LIMIT 5") && looser.contains("OFFSET 20"),
            "{looser}"
        );
        assert!(limit_statement("SELECT id FROM t FETCH FIRST 3 ROWS ONLY", 5, pg).is_none());
        assert!(limit_statement("SELECT id FROM t LIMIT 2, 10", 5, SqlDialect::MySql).is_none());
        assert!(limit_statement("SELECT id FROM t LIMIT $1", 5, pg).is_none());
    }

    #[test]
    fn capabilities_follow_the_dialect_and_the_id_column() {
        let identity = || Identity {
            label: "sql#test".into(),
            fingerprint: [0; 32],
        };
        let spec = |dsn: &'static str, id_field: &'static str| SqlSpec {
            dsn,
            query: "SELECT id FROM t",
            id_field,
            score_field: None,
        };
        let pg = SqlRemote::new(spec("postgres://h/db", "id"), identity());
        assert_eq!(pg.capabilities().max_keys(), Some(MAX_SQL_KEYS));
        assert_eq!(pg.capabilities().limit, LimitPushdown::Native);
        let odd_id = SqlRemote::new(spec("postgres://h/db", "user id"), identity());
        assert_eq!(odd_id.capabilities().key_lookup, KeyLookup::Unsupported);
        let my = SqlRemote::new(spec("mysql://h/db", "id"), identity());
        assert!(!my.key_expressible("a\\b"));
        assert!(pg.key_expressible("a\\b"));
    }
}
