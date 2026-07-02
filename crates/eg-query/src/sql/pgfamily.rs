//! Postgres-family **extension parity** (wave 19): recognize the SQL syntax the
//! popular Postgres extensions expose so a client that speaks them Just Works over
//! the pgwire surface, lowering each onto the engine's native machinery.
//!
//!   * **Apache AGE** `cypher('graph', $$ … $$) AS (col type, …)` (CONCEPT:EG-114) —
//!     a set-returning function in a `FROM` clause. `sqlparser` 0.51 cannot parse the
//!     `AS (col type, …)` column-definition list on a table function (it wants a bare
//!     identifier after `AS`), so — exactly like `DROP EXTENSION`/`COPY … FROM STDIN`
//!     — the shape is recognized **textually** before the parser and routed to the
//!     Cypher engine, whose agtype (JSON) result is projected onto the `AS` columns.
//!   * **pgvector index pushdown** (CONCEPT:EG-116) — `CREATE INDEX … USING hnsw|ivfflat
//!     (col vector_l2_ops)` registers an ANN index (again textual: `sqlparser` chokes on
//!     the opclass), and a `ORDER BY col <-> $1 LIMIT k` nearest-neighbour query is
//!     recognized so the top-k search can be pushed down to `eg-ann` instead of the
//!     `EG-115` brute-force `vector_l2()` full-scan.
//!   * **TimescaleDB** (CONCEPT:EG-117) — `create_hypertable('t','ts')` records the
//!     time-partitioning metadata, and `CREATE MATERIALIZED VIEW … WITH
//!     (timescaledb.continuous) AS SELECT time_bucket(…) …` is a continuous aggregate
//!     lowered onto the durable view catalog + `time_bucket` (EG-067 `Op::Window`).
//!   * **ParadeDB** `col @@@ 'query'` BM25 search + `paradedb.score()`/`snippet()`
//!     (CONCEPT:EG-119) — recognized so the lexical search can lower onto `eg-text`'s
//!     BM25 index (the `@@@` operator + `paradedb.*` functions are desugared in
//!     `classify::desugar_vector_ops`).
//!
//! This module is the PURE parse/plan/project layer (no graph, no runtime, no I/O), so
//! it is unit-testable without a socket. The server-side execution facades live in
//! `src/server/wire`.

use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem, SetExpr,
    Statement, TableFactor, UnaryOperator, Value as SqlValue,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::exec::{PgColType, TypedColumn, TypedQueryResult};

// ─────────────────────────────────────────────────────────────────────────────
// Plan structs (the decoded shapes classify produces)
// ─────────────────────────────────────────────────────────────────────────────

/// One `AS (name type)` column of an AGE `cypher()` call (CONCEPT:EG-114). `type_name`
/// is the raw SQL type spelling (`agtype`, `text`, `int`, …); it selects the wire type
/// the projected agtype value is coerced to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CypherColumn {
    pub name: String,
    pub type_name: String,
}

/// A decoded `SELECT <proj> FROM cypher('graph', $$ <cypher> $$) AS (cols…)`
/// (CONCEPT:EG-114). `graph` names the graph the inner Cypher runs against, `cypher`
/// is the Cypher text (the dollar-quoted body), `columns` are the `AS` column defs the
/// agtype result is projected onto positionally, and `projection` is the outer SELECT
/// list (`None` ⇒ `*` ⇒ all `AS` columns; else the named subset in order).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CypherCallPlan {
    pub graph: String,
    pub cypher: String,
    pub columns: Vec<CypherColumn>,
    pub projection: Option<Vec<String>>,
}

/// The pgvector distance metric an ANN index / query uses (CONCEPT:EG-116), keyed off
/// the opclass (`vector_l2_ops` → `L2`, …) and the distance operator (`<->` → `L2`, …).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VectorMetric {
    /// L2 (Euclidean) — opclass `vector_l2_ops`, operator `<->`.
    L2,
    /// Cosine — opclass `vector_cosine_ops`, operator `<=>`.
    Cosine,
    /// Negative inner product — opclass `vector_ip_ops`, operator `<#>`.
    InnerProduct,
}

/// The ANN index method (CONCEPT:EG-116). Both lower onto the same `eg-ann` IVF-PQ /
/// HNSW backend; the spelling is recorded for catalog fidelity + `EXPLAIN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AnnMethod {
    Hnsw,
    IvfFlat,
}

/// A decoded `CREATE INDEX [IF NOT EXISTS] [name] ON table USING hnsw|ivfflat
/// (col opclass)` (CONCEPT:EG-116) — an ANN index registration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnIndexPlan {
    pub name: Option<String>,
    pub table: String,
    pub column: String,
    pub method: AnnMethod,
    pub metric: VectorMetric,
    pub if_not_exists: bool,
}

/// A decoded `SELECT create_hypertable('t','ts'[, …])` (CONCEPT:EG-117) — the
/// time-partitioning declaration. Extra chunk-interval args are accepted but not
/// required (recorded as metadata; chunk sizing is a documented follow-up).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HypertablePlan {
    pub table: String,
    pub time_column: String,
}

/// A decoded `CREATE MATERIALIZED VIEW [IF NOT EXISTS] name WITH
/// (timescaledb.continuous) AS <select>` (CONCEPT:EG-117) — a continuous aggregate.
/// `select_sql` is the aggregate SELECT (typically a `time_bucket` GROUP BY) lowered
/// onto the durable view catalog; incremental materialized refresh is a follow-up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinuousAggPlan {
    pub name: String,
    pub select_sql: String,
    pub if_not_exists: bool,
}

/// A recognized pgvector nearest-neighbour query eligible for ANN index pushdown
/// (CONCEPT:EG-116): `… FROM table [WHERE …] ORDER BY column <op> <query> LIMIT k`.
/// `query` is the query-vector operand as SQL text (a `$N` placeholder or a `'[…]'`
/// pgvector literal) — the server resolves it and calls `eg-ann::search(query, k)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnSearchPlan {
    pub table: String,
    pub column: String,
    pub metric: VectorMetric,
    pub k: usize,
    pub query: String,
}

/// A recognized ParadeDB BM25 search (CONCEPT:EG-119): `… FROM table WHERE column @@@
/// 'query' [ORDER BY paradedb.score(...) DESC] [LIMIT k]`. The server lowers it onto
/// `eg-text`'s BM25 index (`TextIndex::search(query, k)`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bm25SearchPlan {
    pub table: String,
    pub column: String,
    pub query: String,
    pub k: Option<usize>,
}

// ─────────────────────────────────────────────────────────────────────────────
// EG-114 — textual recognition of the AGE cypher() table function
// ─────────────────────────────────────────────────────────────────────────────

/// Recognize `SELECT <proj> FROM cypher('graph', $tag$ <cypher> $tag$) AS (cols…)`
/// (CONCEPT:EG-114) textually, BEFORE the parser (which cannot parse the typed `AS`
/// column list on a table function). Returns `None` for any statement that is not this
/// exact AGE shape, so `classify` falls through to the ordinary read path.
pub fn parse_cypher_call(sql: &str) -> Option<CypherCallPlan> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("select") {
        return None;
    }
    // The outer projection is everything between SELECT and FROM; the table function
    // follows the FROM.
    let from_pos = lower.find(" from ")?;
    let proj_text = trimmed["select".len()..from_pos].trim();
    let projection = parse_projection_list(proj_text);

    let after_from = trimmed[from_pos + 6..].trim_start();
    if after_from.len() < 6 || !after_from[..6].eq_ignore_ascii_case("cypher") {
        return None;
    }
    // `rest` starts at the `(` after `cypher`.
    let rest = after_from[6..].trim_start();
    if !rest.starts_with('(') {
        return None;
    }

    // ( 'graph' , $$ cypher $$ )
    let mut idx = skip_ws(rest, 1);
    let (graph, ni) = read_squote(rest, idx)?;
    idx = skip_ws(rest, ni);
    if rest.as_bytes().get(idx)? != &b',' {
        return None;
    }
    idx = skip_ws(rest, idx + 1);
    let (cypher, ni) = read_dollar_quoted(rest, idx)?;
    idx = skip_ws(rest, ni);
    if rest.as_bytes().get(idx)? != &b')' {
        return None;
    }
    idx = skip_ws(rest, idx + 1);

    // AS ( col type, … )
    let tail = &rest[idx..];
    if tail.len() < 2 || !tail[..2].eq_ignore_ascii_case("as") {
        return None;
    }
    let after_as = tail[2..].trim_start();
    if !after_as.starts_with('(') {
        return None;
    }
    let cols_inner = &after_as[1..];
    let close = cols_inner.rfind(')')?;
    let columns = parse_column_defs(&cols_inner[..close])?;
    if columns.is_empty() {
        return None;
    }
    Some(CypherCallPlan {
        graph,
        cypher: cypher.trim().to_string(),
        columns,
        projection,
    })
}

/// Parse the outer SELECT projection list: `*` ⇒ `None` (all columns), else a simple
/// comma-list of column names ⇒ `Some(names)`. Anything richer (expressions, aliases)
/// falls back to `None` (return all AS columns) — a documented limitation.
fn parse_projection_list(proj: &str) -> Option<Vec<String>> {
    let p = proj.trim();
    if p == "*" || p.is_empty() {
        return None;
    }
    let mut names = Vec::new();
    for part in p.split(',') {
        let t = part.trim();
        // Only bare identifiers are projected; anything else ⇒ fall back to all.
        if t.is_empty() || !t.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return None;
        }
        names.push(t.to_string());
    }
    Some(names)
}

/// Split `name type[, name type…]` column-definition text into typed columns
/// (CONCEPT:EG-114). The first token of each part is the name; the remainder is the
/// (possibly multi-word, e.g. `double precision`) type spelling.
fn parse_column_defs(inner: &str) -> Option<Vec<CypherColumn>> {
    let mut cols = Vec::new();
    for part in inner.split(',') {
        let mut it = part.split_whitespace();
        let name = it.next()?.to_string();
        let type_name = it.collect::<Vec<_>>().join(" ");
        let type_name = if type_name.is_empty() {
            "agtype".to_string()
        } else {
            type_name
        };
        cols.push(CypherColumn { name, type_name });
    }
    Some(cols)
}

/// Byte index of the next non-whitespace char at or after `from`.
fn skip_ws(s: &str, from: usize) -> usize {
    let bytes = s.as_bytes();
    let mut i = from;
    while i < bytes.len() && (bytes[i] as char).is_whitespace() {
        i += 1;
    }
    i
}

/// Read a single-quoted string starting at byte `start` (which must be `'`). Returns
/// the unescaped contents and the byte index just past the closing quote. `''` is an
/// escaped quote.
fn read_squote(s: &str, start: usize) -> Option<(String, usize)> {
    let bytes = s.as_bytes();
    if *bytes.get(start)? != b'\'' {
        return None;
    }
    let mut out = String::new();
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if bytes.get(i + 1) == Some(&b'\'') {
                out.push('\'');
                i += 2;
                continue;
            }
            return Some((out, i + 1));
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    None
}

/// Read a dollar-quoted string (`$$ … $$` or `$tag$ … $tag$`) starting at byte
/// `start` (which must be `$`). Returns the body and the byte index just past the
/// closing tag. This mirrors Postgres dollar-quoting, so the Cypher body may contain
/// parens, commas, and single quotes freely.
pub(super) fn read_dollar_quoted(s: &str, start: usize) -> Option<(String, usize)> {
    let bytes = s.as_bytes();
    if *bytes.get(start)? != b'$' {
        return None;
    }
    // Read the opening tag `$tag$`.
    let mut i = start + 1;
    while i < bytes.len() && bytes[i] != b'$' {
        i += 1;
    }
    if i >= bytes.len() {
        return None;
    }
    let tag = &s[start..=i]; // includes both `$`
    let body_start = i + 1;
    let close = s[body_start..].find(tag)?;
    let body = &s[body_start..body_start + close];
    Some((body.to_string(), body_start + close + tag.len()))
}

// ─────────────────────────────────────────────────────────────────────────────
// EG-114 — project an agtype (JSON) Cypher result onto the AS columns
// ─────────────────────────────────────────────────────────────────────────────

/// Project a Cypher engine result (`columns` + MessagePack-encoded JSON rows) onto the
/// AGE `AS (col type, …)` column list (CONCEPT:EG-114), coercing each value to the
/// declared wire type and applying the outer projection. Positional: the Nth Cypher
/// RETURN item fills the Nth `AS` column (AGE semantics).
pub fn project_cypher_rows(
    cypher_result: &eg_types::protocol::QueryResult,
    columns: &[CypherColumn],
    projection: Option<&[String]>,
) -> Result<TypedQueryResult, String> {
    // Resolve the output column order from the projection (default: all AS columns).
    let out_idx: Vec<usize> = match projection {
        None => (0..columns.len()).collect(),
        Some(names) => names
            .iter()
            .map(|n| {
                columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(n))
                    .ok_or_else(|| format!("cypher(): projected column `{n}` not in AS list"))
            })
            .collect::<Result<_, _>>()?,
    };
    let out_columns: Vec<TypedColumn> = out_idx
        .iter()
        .map(|&i| TypedColumn {
            name: columns[i].name.clone(),
            ty: pg_type_of(&columns[i].type_name),
        })
        .collect();

    let mut rows = Vec::with_capacity(cypher_result.rows.len());
    for raw in &cypher_result.rows {
        let cells: Vec<Value> =
            rmp_serde::from_slice(raw).map_err(|e| format!("cypher(): decode row: {e}"))?;
        let mut out_row = Vec::with_capacity(out_idx.len());
        for &i in &out_idx {
            let v = cells.get(i).cloned().unwrap_or(Value::Null);
            out_row.push(coerce_value(v, &columns[i].type_name));
        }
        rows.push(out_row);
    }
    Ok(TypedQueryResult {
        columns: out_columns,
        rows,
    })
}

/// The typed output columns an AGE `cypher()` call produces (CONCEPT:EG-114) — the
/// `AS` column list narrowed by the outer projection. Used by the pgwire Describe step
/// (extended protocol) to report the `RowDescription` WITHOUT executing the Cypher.
/// Lenient: an unknown projected name is dropped (Describe never errors).
pub fn cypher_output_columns(plan: &CypherCallPlan) -> Vec<TypedColumn> {
    let idx: Vec<usize> = match &plan.projection {
        None => (0..plan.columns.len()).collect(),
        Some(names) => names
            .iter()
            .filter_map(|n| {
                plan.columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(n))
            })
            .collect(),
    };
    idx.into_iter()
        .map(|i| TypedColumn {
            name: plan.columns[i].name.clone(),
            ty: pg_type_of(&plan.columns[i].type_name),
        })
        .collect()
}

/// Map a raw SQL type spelling to the coarse pg-mappable wire type (CONCEPT:EG-114).
/// AGE's `agtype` (and any JSON-ish type) surfaces as text.
pub(crate) fn pg_type_of(type_name: &str) -> PgColType {
    match type_name.trim().to_ascii_lowercase().as_str() {
        "int" | "integer" | "int2" | "int4" | "int8" | "bigint" | "smallint" | "serial"
        | "bigserial" => PgColType::Int8,
        "float" | "float4" | "float8" | "double" | "double precision" | "real" | "numeric"
        | "decimal" => PgColType::Float8,
        "bool" | "boolean" => PgColType::Bool,
        "vector" => PgColType::Vector,
        _ => PgColType::Text, // agtype, text, varchar, json, jsonb, uuid, …
    }
}

/// Coerce a decoded agtype JSON value to the declared column type (CONCEPT:EG-114).
/// Lenient: an un-coercible value is preserved as-is rather than erroring, so a partial
/// row still renders.
fn coerce_value(v: Value, type_name: &str) -> Value {
    match pg_type_of(type_name) {
        PgColType::Int8 => match &v {
            // A float agtype value (e.g. `25.0`) truncates to an integer, like pg.
            Value::Number(_) => v
                .as_i64()
                .or_else(|| v.as_f64().map(|f| f as i64))
                .map(Value::from)
                .unwrap_or(v),
            Value::String(s) => s.parse::<i64>().ok().map(Value::from).unwrap_or(v),
            _ => v,
        },
        PgColType::Float8 => match &v {
            Value::Number(_) => v.as_f64().map(Value::from).unwrap_or(v),
            Value::String(s) => s.parse::<f64>().ok().map(Value::from).unwrap_or(v),
            _ => v,
        },
        PgColType::Bool => match &v {
            Value::Bool(_) => v,
            Value::String(s) => s.parse::<bool>().ok().map(Value::from).unwrap_or(v),
            _ => v,
        },
        PgColType::Vector => v,
        PgColType::Text => match v {
            Value::Null => Value::Null,
            Value::String(s) => Value::String(s),
            // agtype ⇒ text is the JSON serialization of the value.
            other => Value::String(other.to_string()),
        },
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EG-116 — textual recognition of CREATE INDEX … USING hnsw|ivfflat
// ─────────────────────────────────────────────────────────────────────────────

/// Recognize `CREATE INDEX [CONCURRENTLY] [IF NOT EXISTS] [name] ON table USING
/// hnsw|ivfflat (col [opclass])` (CONCEPT:EG-116) textually — `sqlparser` 0.51 cannot
/// parse the opclass inside the column list, nor `IF NOT EXISTS` on an index. Returns
/// `None` for a non-ANN `CREATE INDEX` (so `classify` falls through to its ordinary —
/// currently-unsupported — path) or any other statement.
pub fn parse_create_ann_index(sql: &str) -> Option<AnnIndexPlan> {
    let toks = tokenize(sql);
    let mut i = 0;
    let eat = |toks: &[String], i: &mut usize, kw: &str| -> bool {
        if toks.get(*i).map(|t| t.eq_ignore_ascii_case(kw)) == Some(true) {
            *i += 1;
            true
        } else {
            false
        }
    };
    if !eat(&toks, &mut i, "create") || !eat(&toks, &mut i, "index") {
        return None;
    }
    let _ = eat(&toks, &mut i, "concurrently");
    let mut if_not_exists = false;
    if toks.get(i).map(|t| t.eq_ignore_ascii_case("if")) == Some(true) {
        // `IF NOT EXISTS`
        if toks.get(i + 1).map(|t| t.eq_ignore_ascii_case("not")) == Some(true)
            && toks.get(i + 2).map(|t| t.eq_ignore_ascii_case("exists")) == Some(true)
        {
            if_not_exists = true;
            i += 3;
        } else {
            return None;
        }
    }
    // Optional index name (an identifier that is not `ON`).
    let mut name = None;
    if let Some(t) = toks.get(i) {
        if !t.eq_ignore_ascii_case("on") {
            name = Some(t.clone());
            i += 1;
        }
    }
    if !eat(&toks, &mut i, "on") {
        return None;
    }
    let table = toks.get(i)?.clone();
    i += 1;
    if !eat(&toks, &mut i, "using") {
        return None;
    }
    let method = match toks.get(i)?.to_ascii_lowercase().as_str() {
        "hnsw" => AnnMethod::Hnsw,
        "ivfflat" => AnnMethod::IvfFlat,
        _ => return None,
    };
    i += 1;
    if toks.get(i)? != "(" {
        return None;
    }
    i += 1;
    let column = toks.get(i)?.clone();
    i += 1;
    // Optional opclass ⇒ metric (default L2).
    let mut metric = VectorMetric::L2;
    if let Some(t) = toks.get(i) {
        if t != ")" {
            metric = metric_from_opclass(t);
            i += 1;
        }
    }
    if toks.get(i)? != ")" {
        return None;
    }
    Some(AnnIndexPlan {
        name,
        table,
        column,
        method,
        metric,
        if_not_exists,
    })
}

/// Map a pgvector opclass name to its distance metric (CONCEPT:EG-116).
fn metric_from_opclass(op: &str) -> VectorMetric {
    match op.to_ascii_lowercase().as_str() {
        "vector_cosine_ops" => VectorMetric::Cosine,
        "vector_ip_ops" => VectorMetric::InnerProduct,
        _ => VectorMetric::L2, // vector_l2_ops + default
    }
}

/// Tokenize SQL into words + the punctuation `(` `)` `,`, honouring single-quoted
/// strings as a single token. Lightweight — enough for the textual DDL recognizers.
fn tokenize(sql: &str) -> Vec<String> {
    let s = sql.trim().trim_end_matches(';');
    let bytes = s.as_bytes();
    let mut toks = Vec::new();
    let mut cur = String::new();
    let mut i = 0;
    let flush = |cur: &mut String, toks: &mut Vec<String>| {
        if !cur.is_empty() {
            toks.push(std::mem::take(cur));
        }
    };
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c == '\'' {
            flush(&mut cur, &mut toks);
            let mut lit = String::from("'");
            i += 1;
            while i < bytes.len() {
                lit.push(bytes[i] as char);
                if bytes[i] == b'\'' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            toks.push(lit);
            continue;
        }
        if c.is_whitespace() {
            flush(&mut cur, &mut toks);
        } else if c == '(' || c == ')' || c == ',' {
            flush(&mut cur, &mut toks);
            toks.push(c.to_string());
        } else {
            cur.push(c);
        }
        i += 1;
    }
    flush(&mut cur, &mut toks);
    toks
}

// ─────────────────────────────────────────────────────────────────────────────
// EG-117 — create_hypertable() + continuous aggregate
// ─────────────────────────────────────────────────────────────────────────────

/// Detect `SELECT create_hypertable('t','ts'[, …])` (CONCEPT:EG-117) in a parsed query
/// and extract the table + time column. Returns `None` for any other query so the
/// ordinary read path applies.
pub fn detect_create_hypertable(stmt: &Statement) -> Option<HypertablePlan> {
    let Statement::Query(q) = stmt else {
        return None;
    };
    let SetExpr::Select(select) = q.body.as_ref() else {
        return None;
    };
    if !select.from.is_empty() {
        return None;
    }
    let [SelectItem::UnnamedExpr(Expr::Function(f))] = select.projection.as_slice() else {
        return None;
    };
    if !last_fn_ident(f).eq_ignore_ascii_case("create_hypertable") {
        return None;
    }
    let FunctionArguments::List(list) = &f.args else {
        return None;
    };
    let mut strs = list.args.iter().filter_map(arg_as_string);
    let table = strs.next()?;
    let time_column = strs.next()?;
    Some(HypertablePlan { table, time_column })
}

/// Recognize `CREATE MATERIALIZED VIEW [IF NOT EXISTS] name WITH (timescaledb.continuous)
/// AS <select>` (CONCEPT:EG-117) textually — `sqlparser` cannot parse the dotted
/// `timescaledb.continuous` option. Returns `None` for a plain materialized view (which
/// the parser then rejects as a documented follow-up).
pub fn parse_continuous_aggregate(sql: &str) -> Option<ContinuousAggPlan> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    let after = lower.strip_prefix("create materialized view")?;
    // Must be a timescaledb continuous aggregate.
    let cont_pos = lower.find("timescaledb.continuous")?;
    let mut rest = after.trim_start();
    let mut if_not_exists = false;
    if let Some(r) = rest.strip_prefix("if not exists") {
        if_not_exists = true;
        rest = r.trim_start();
    }
    // Name is the next token; extract from `trimmed` at the same byte offset (ASCII).
    let name: String = trimmed[trimmed.len() - rest.len()..]
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if name.is_empty() {
        return None;
    }
    // The SELECT is everything after the first ` AS ` that follows the WITH clause.
    let as_search_from = cont_pos;
    let rel = lower[as_search_from..].find(" as ")?;
    let select_start = as_search_from + rel + 4;
    let mut select_sql = trimmed[select_start..].trim().to_string();
    // Strip a trailing `WITH [NO] DATA`.
    let sl = select_sql.to_ascii_lowercase();
    if let Some(p) = sl.rfind("with data").or_else(|| sl.rfind("with no data")) {
        // only strip if it is a trailing clause
        if sl[p..].trim_end() == "with data" || sl[p..].trim_end() == "with no data" {
            select_sql = select_sql[..p].trim_end().to_string();
        }
    }
    if select_sql.is_empty() {
        return None;
    }
    Some(ContinuousAggPlan {
        name,
        select_sql,
        if_not_exists,
    })
}

/// The last identifier of a function name (`paradedb.score` → `score`,
/// `create_hypertable` → `create_hypertable`).
fn last_fn_ident(f: &datafusion::sql::sqlparser::ast::Function) -> String {
    f.name.0.last().map(|i| i.value.clone()).unwrap_or_default()
}

/// A function argument that is a single-quoted string literal ⇒ its contents.
fn arg_as_string(arg: &FunctionArg) -> Option<String> {
    let expr = match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
        FunctionArg::Named {
            arg: FunctionArgExpr::Expr(e),
            ..
        } => e,
        _ => return None,
    };
    match expr {
        Expr::Value(SqlValue::SingleQuotedString(s)) => Some(s.clone()),
        _ => None,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EG-116 — ANN nearest-neighbour pushdown planner
// ─────────────────────────────────────────────────────────────────────────────

/// Recognize a pgvector nearest-neighbour query eligible for ANN index pushdown
/// (CONCEPT:EG-116) and return its [`AnnSearchPlan`] IFF a registered `indexes` entry
/// covers the `(table, column, metric)` — i.e. the query "chooses ANN". With no
/// matching index the caller falls back to the EG-115 brute-force `vector_l2()` scan.
///
/// Recognized shape: a single-table `SELECT … ORDER BY column <op> <query> LIMIT k`,
/// where `<op>` is a pgvector distance operator (`<->`/`<=>`/`<#>`).
pub fn plan_ann_search(sql: &str, indexes: &[AnnIndexPlan]) -> Option<AnnSearchPlan> {
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    let [Statement::Query(q)] = stmts.as_slice() else {
        return None;
    };
    // LIMIT k (required — ANN is a top-k search).
    let k = match q.limit.as_ref()? {
        Expr::Value(SqlValue::Number(n, _)) => n.parse::<usize>().ok()?,
        _ => return None,
    };
    // ORDER BY column <op> query.
    let order_by = q.order_by.as_ref()?;
    let [ob] = order_by.exprs.as_slice() else {
        return None;
    };
    let (column, metric, query) = vector_order_key(&ob.expr)?;
    // FROM must be a single bare table.
    let SetExpr::Select(select) = q.body.as_ref() else {
        return None;
    };
    let [twj] = select.from.as_slice() else {
        return None;
    };
    let TableFactor::Table { name, .. } = &twj.relation else {
        return None;
    };
    let table = name.0.last()?.value.clone();
    // "Choose ANN" only when an index covers (table, column, metric).
    let covered = indexes.iter().any(|ix| {
        ix.table.eq_ignore_ascii_case(&table)
            && ix.column.eq_ignore_ascii_case(&column)
            && ix.metric == metric
    });
    if !covered {
        return None;
    }
    Some(AnnSearchPlan {
        table,
        column,
        metric,
        k,
        query,
    })
}

/// Decode an `ORDER BY column <-> query` key: returns `(column, metric, query_text)`
/// for a pgvector distance operator, else `None` (CONCEPT:EG-116).
fn vector_order_key(expr: &Expr) -> Option<(String, VectorMetric, String)> {
    let Expr::BinaryOp { left, op, right } = expr else {
        return None;
    };
    let metric = match op {
        BinaryOperator::Custom(s) if s == "<->" => VectorMetric::L2,
        BinaryOperator::Custom(s) if s == "<#>" => VectorMetric::InnerProduct,
        BinaryOperator::Spaceship => VectorMetric::Cosine, // `<=>`
        _ => return None,
    };
    let column = match left.as_ref() {
        Expr::Identifier(id) => id.value.clone(),
        Expr::CompoundIdentifier(ids) => ids.last()?.value.clone(),
        _ => return None,
    };
    Some((column, metric, right.to_string()))
}

// ─────────────────────────────────────────────────────────────────────────────
// EG-119 — ParadeDB BM25 search planner
// ─────────────────────────────────────────────────────────────────────────────

/// Recognize a ParadeDB BM25 search (CONCEPT:EG-119): `… FROM table WHERE column @@@
/// 'query' [LIMIT k]`. Returns the [`Bm25SearchPlan`] the server lowers onto `eg-text`.
/// `@@@` tokenizes in `sqlparser` as `AtAt` with a `PGAbs`-wrapped right operand.
pub fn plan_bm25_search(sql: &str) -> Option<Bm25SearchPlan> {
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    let [Statement::Query(q)] = stmts.as_slice() else {
        return None;
    };
    let SetExpr::Select(select) = q.body.as_ref() else {
        return None;
    };
    let [twj] = select.from.as_slice() else {
        return None;
    };
    let TableFactor::Table { name, .. } = &twj.relation else {
        return None;
    };
    let table = name.0.last()?.value.clone();
    let (column, query) = find_bm25_match(select.selection.as_ref()?)?;
    let k = match q.limit.as_ref() {
        Some(Expr::Value(SqlValue::Number(n, _))) => n.parse::<usize>().ok(),
        _ => None,
    };
    Some(Bm25SearchPlan {
        table,
        column,
        query,
        k,
    })
}

/// Find a `column @@@ 'query'` match anywhere in a WHERE predicate (CONCEPT:EG-119),
/// descending through `AND`/`OR`. Returns `(column, query_text)`.
fn find_bm25_match(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::AtAt,
            right,
        } => {
            // `@@@` ⇒ right is a `@`(PGAbs)-wrapped string literal.
            let inner = match right.as_ref() {
                Expr::UnaryOp {
                    op: UnaryOperator::PGAbs,
                    expr,
                } => expr.as_ref(),
                other => other,
            };
            let column = match left.as_ref() {
                Expr::Identifier(id) => id.value.clone(),
                Expr::CompoundIdentifier(ids) => ids.last()?.value.clone(),
                _ => return None,
            };
            let query = match inner {
                Expr::Value(SqlValue::SingleQuotedString(s)) => s.clone(),
                other => other.to_string(),
            };
            Some((column, query))
        }
        Expr::BinaryOp { left, right, .. } => {
            find_bm25_match(left).or_else(|| find_bm25_match(right))
        }
        Expr::Nested(inner) => find_bm25_match(inner),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── EG-114: AGE cypher() recognition + projection ──────────────────────────
    #[test]
    fn eg114_parse_cypher_call_typed_as_columns() {
        let plan = parse_cypher_call(
            "SELECT * FROM cypher('social', $$ MATCH (n:Person) RETURN n.id, n.name $$) \
             AS (id agtype, name text)",
        )
        .expect("recognized");
        assert_eq!(plan.graph, "social");
        assert_eq!(plan.cypher, "MATCH (n:Person) RETURN n.id, n.name");
        assert_eq!(plan.columns.len(), 2);
        assert_eq!(plan.columns[0].name, "id");
        assert_eq!(plan.columns[1].type_name, "text");
        assert_eq!(plan.projection, None);
    }

    #[test]
    fn eg114_cypher_call_projection_subset() {
        let plan = parse_cypher_call(
            "SELECT name FROM cypher('g', $$ MATCH (n) RETURN n.id, n.name $$) \
             AS (id agtype, name text)",
        )
        .expect("recognized");
        assert_eq!(plan.projection, Some(vec!["name".to_string()]));
    }

    #[test]
    fn eg114_non_cypher_select_is_not_recognized() {
        assert!(parse_cypher_call("SELECT * FROM nodes").is_none());
    }

    #[test]
    fn eg114_project_agtype_rows_onto_typed_columns() {
        let columns = vec![
            CypherColumn {
                name: "id".into(),
                type_name: "agtype".into(),
            },
            CypherColumn {
                name: "age".into(),
                type_name: "int".into(),
            },
        ];
        // Two Cypher rows: ["alice", 30], ["bob", 25.0] (agtype/JSON).
        let rows = vec![
            rmp_serde::to_vec(&vec![Value::from("alice"), Value::from(30)]).unwrap(),
            rmp_serde::to_vec(&vec![Value::from("bob"), Value::from(25.0)]).unwrap(),
        ];
        let cypher_result = eg_types::protocol::QueryResult {
            columns: vec!["n.id".into(), "n.age".into()],
            rows,
        };
        let out = project_cypher_rows(&cypher_result, &columns, None).unwrap();
        assert_eq!(out.columns.len(), 2);
        assert_eq!(out.columns[0].ty, PgColType::Text);
        assert_eq!(out.columns[1].ty, PgColType::Int8);
        assert_eq!(out.rows[0][0], Value::from("alice"));
        assert_eq!(out.rows[0][1], Value::from(30i64));
        // 25.0 coerces to int 25.
        assert_eq!(out.rows[1][1], Value::from(25i64));
    }

    #[test]
    fn eg114_project_applies_column_subset() {
        let columns = vec![
            CypherColumn {
                name: "id".into(),
                type_name: "agtype".into(),
            },
            CypherColumn {
                name: "name".into(),
                type_name: "text".into(),
            },
        ];
        let rows = vec![rmp_serde::to_vec(&vec![Value::from(1), Value::from("x")]).unwrap()];
        let cr = eg_types::protocol::QueryResult {
            columns: vec!["a".into(), "b".into()],
            rows,
        };
        let out = project_cypher_rows(&cr, &columns, Some(&["name".to_string()])).unwrap();
        assert_eq!(out.columns.len(), 1);
        assert_eq!(out.columns[0].name, "name");
        assert_eq!(out.rows[0][0], Value::from("x"));
    }

    // ── EG-116: pgvector CREATE INDEX + ANN pushdown ───────────────────────────
    #[test]
    fn eg116_parse_create_hnsw_index() {
        let plan =
            parse_create_ann_index("CREATE INDEX ON items USING hnsw (embedding vector_l2_ops)")
                .expect("recognized");
        assert_eq!(plan.table, "items");
        assert_eq!(plan.column, "embedding");
        assert_eq!(plan.method, AnnMethod::Hnsw);
        assert_eq!(plan.metric, VectorMetric::L2);
    }

    #[test]
    fn eg116_parse_ivfflat_index_if_not_exists_named_cosine() {
        let plan = parse_create_ann_index(
            "CREATE INDEX IF NOT EXISTS emb_idx ON docs USING ivfflat (emb vector_cosine_ops)",
        )
        .expect("recognized");
        assert_eq!(plan.name.as_deref(), Some("emb_idx"));
        assert_eq!(plan.table, "docs");
        assert_eq!(plan.method, AnnMethod::IvfFlat);
        assert_eq!(plan.metric, VectorMetric::Cosine);
        assert!(plan.if_not_exists);
    }

    #[test]
    fn eg116_non_ann_index_is_not_recognized() {
        assert!(parse_create_ann_index("CREATE INDEX ON t USING btree (a)").is_none());
    }

    #[test]
    fn eg116_ann_pushdown_chooses_ann_when_index_present() {
        let indexes = vec![AnnIndexPlan {
            name: None,
            table: "items".into(),
            column: "embedding".into(),
            method: AnnMethod::Hnsw,
            metric: VectorMetric::L2,
            if_not_exists: false,
        }];
        let sql = "SELECT id FROM items ORDER BY embedding <-> '[1,2,3]' LIMIT 5";
        let plan = plan_ann_search(sql, &indexes).expect("chooses ANN");
        assert_eq!(plan.table, "items");
        assert_eq!(plan.column, "embedding");
        assert_eq!(plan.metric, VectorMetric::L2);
        assert_eq!(plan.k, 5);
        assert_eq!(plan.query, "'[1,2,3]'");
    }

    #[test]
    fn eg116_ann_pushdown_falls_back_without_index() {
        // No registered index ⇒ do NOT choose ANN (brute-force EG-115 path applies).
        let sql = "SELECT id FROM items ORDER BY embedding <-> '[1,2,3]' LIMIT 5";
        assert!(plan_ann_search(sql, &[]).is_none());
    }

    // ── EG-117: TimescaleDB hypertable + continuous aggregate ──────────────────
    #[test]
    fn eg117_detect_create_hypertable() {
        let stmts = Parser::parse_sql(
            &PostgreSqlDialect {},
            "SELECT create_hypertable('conditions', 'ts')",
        )
        .unwrap();
        let plan = detect_create_hypertable(&stmts[0]).expect("recognized");
        assert_eq!(plan.table, "conditions");
        assert_eq!(plan.time_column, "ts");
    }

    #[test]
    fn eg117_plain_select_is_not_a_hypertable() {
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, "SELECT 1").unwrap();
        assert!(detect_create_hypertable(&stmts[0]).is_none());
    }

    #[test]
    fn eg117_parse_continuous_aggregate() {
        let plan = parse_continuous_aggregate(
            "CREATE MATERIALIZED VIEW cagg WITH (timescaledb.continuous) AS \
             SELECT time_bucket('1 hour', ts) AS bucket, avg(v) FROM conditions GROUP BY bucket",
        )
        .expect("recognized");
        assert_eq!(plan.name, "cagg");
        assert!(plan
            .select_sql
            .to_ascii_lowercase()
            .starts_with("select time_bucket"));
        assert!(!plan.if_not_exists);
    }

    #[test]
    fn eg117_plain_materialized_view_is_not_continuous() {
        assert!(parse_continuous_aggregate(
            "CREATE MATERIALIZED VIEW v AS SELECT * FROM conditions"
        )
        .is_none());
    }

    // ── EG-119: ParadeDB BM25 @@@ search ───────────────────────────────────────
    #[test]
    fn eg119_plan_bm25_search_filter_and_limit() {
        let plan =
            plan_bm25_search("SELECT id FROM docs WHERE body @@@ 'quantum computing' LIMIT 10")
                .expect("recognized");
        assert_eq!(plan.table, "docs");
        assert_eq!(plan.column, "body");
        assert_eq!(plan.query, "quantum computing");
        assert_eq!(plan.k, Some(10));
    }

    #[test]
    fn eg119_plan_bm25_search_within_and() {
        let plan = plan_bm25_search("SELECT id FROM docs WHERE lang = 'en' AND body @@@ 'rust'")
            .expect("recognized");
        assert_eq!(plan.column, "body");
        assert_eq!(plan.query, "rust");
        assert_eq!(plan.k, None);
    }

    #[test]
    fn eg119_non_bm25_where_is_not_recognized() {
        assert!(plan_bm25_search("SELECT id FROM docs WHERE lang = 'en'").is_none());
    }

    // ── EG-117 + EG-119: end-to-end basic exec of the new UDFs through DataFusion ──
    #[test]
    fn eg117_eg119_time_bucket_and_bm25_match_execute() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("body", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![0i64, 3_700, 7_300])),
                Arc::new(StringArray::from(vec![
                    "rust is great",
                    "go lang",
                    "rust rocks",
                ])),
            ],
        )
        .unwrap();

        // CONCEPT:EG-119 `@@@` desugars to bm25_match (filter); CONCEPT:EG-117
        // time_bucket floors the timestamp — both run in one DataFusion plan.
        let out = crate::exec_sql_over_tables(
            vec![("events".to_string(), schema, vec![batch])],
            "SELECT time_bucket(3600, ts) AS bucket FROM events \
             WHERE body @@@ 'rust' ORDER BY bucket",
        )
        .unwrap();
        assert_eq!(out.rows.len(), 2, "only the two 'rust' rows match");
        assert_eq!(out.rows[0][0], Value::from(0i64));
        assert_eq!(out.rows[1][0], Value::from(7_200i64));
    }

    /// CONCEPT:EG-311 — REAL BM25 ranking + highlighted snippets through DataFusion.
    /// The 2-arg `bm25_score(body, 'query')` / `bm25_snippet(body, 'query', n)` forms
    /// carry the query into the per-row UDF, so `ORDER BY bm25_score(...) DESC` puts the
    /// more-relevant document FIRST (not the constant-1.0 placeholder order) and the
    /// snippet wraps matched terms in `<b>…</b>`.
    #[test]
    fn eg311_bm25_real_score_orders_and_snippet_highlights() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["low", "high"])),
                Arc::new(StringArray::from(vec![
                    "the quick brown fox jumps over the lazy dog just once",
                    "the lazy dog naps while another lazy dog guards the dog house",
                ])),
            ],
        )
        .unwrap();

        let out = crate::exec_sql_over_tables(
            vec![("docs".to_string(), schema, vec![batch])],
            "SELECT id, bm25_snippet(body, 'lazy dog', 40) AS snip \
             FROM docs WHERE body @@@ 'lazy dog' \
             ORDER BY bm25_score(body, 'lazy dog') DESC",
        )
        .unwrap();

        assert_eq!(out.rows.len(), 2, "both docs contain the terms");
        // The doc with more 'lazy dog' occurrences ranks FIRST — real BM25, not 1.0.
        assert_eq!(
            out.rows[0][0],
            Value::from("high"),
            "more-relevant doc ranked first: {:?}",
            out.rows
        );
        assert_eq!(out.rows[1][0], Value::from("low"));
        // The snippet highlights the matched terms.
        let snip = out.rows[0][1].as_str().unwrap_or_default();
        assert!(
            snip.contains("<b>lazy</b>") || snip.contains("<b>dog</b>"),
            "snippet highlights a matched term: {snip:?}"
        );
    }
}
