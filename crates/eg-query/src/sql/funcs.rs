//! SQL stored-function EXPANSION (CONCEPT:EG-118). `CREATE FUNCTION` persists a
//! `LANGUAGE sql` function in the durable catalog (`tables::store`); at plan time a
//! call is EXPANDED into the query text so DataFusion's existing planner executes it —
//! there is deliberately NO separate function evaluator (the same "reuse the SQL exec"
//! discipline the view catalog uses).
//!
//! Two shapes, both a pure text→(text|AST)→text rewrite applied to a read query BEFORE
//! it is planned (exactly like `classify::desugar_vector_ops` / `exec::register_views`):
//!
//!   * **scalar** `RETURNS <type> AS $$ SELECT <expr> $$` — a call `fn(a, b)` in ANY
//!     expression position is replaced by a scalar subquery `(<body> with args
//!     substituted>)`. Because the body is a single-row/single-column `SELECT`, the
//!     subquery is valid wherever an expression is, and DataFusion computes the value.
//!     Substitution is whole-word, quote-aware, and PARENTHESIZES each argument so
//!     operator precedence is preserved (`a * b` with `a := 1+2` → `(1+2) * b`).
//!
//!   * **table** `RETURNS TABLE(...)/SETOF … AS $$ SELECT … $$` — a `FROM fn(a, b)`
//!     reference is replaced (at the AST level, walking only the `FROM` clauses) by the
//!     body `SELECT` as a derived subquery — a parameterized view. The original alias is
//!     preserved (defaulting to the function name).
//!
//! Multiple passes (bounded by [`MAX_PASSES`]) resolve nested/among-argument calls and
//! functions that call other functions; a self-referential (recursive) definition is
//! left partially expanded after the bound and DataFusion reports the unknown function —
//! recursive SQL functions are a documented follow-up, as is a procedural PL/pgSQL body.

use datafusion::sql::sqlparser::ast::{
    FunctionArg, FunctionArgExpr, Ident, Query, SetExpr, Statement, TableAlias, TableFactor,
    TableWithJoins,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;

use crate::tables::schema::StoredFunction;

/// Guard against an unbounded rewrite loop from a (rejected-at-runtime) recursive
/// function definition (CONCEPT:EG-118).
const MAX_PASSES: usize = 32;

/// Expand every stored-function call in `sql` into inline SQL (CONCEPT:EG-118). A no-op
/// (returns `sql` verbatim) when there are no functions. Scalar calls are expanded
/// textually; table-function `FROM` references are expanded at the AST level. Loops
/// until a fixed point (or [`MAX_PASSES`]) so nested calls and function-calls-functions
/// resolve. An `Err` is returned only for a genuinely malformed call (argument-count
/// mismatch) — a body that fails to re-parse leaves the text unchanged (DataFusion then
/// reports the precise error).
pub(super) fn expand_functions(sql: &str, functions: &[StoredFunction]) -> Result<String, String> {
    if functions.is_empty() {
        return Ok(sql.to_string());
    }
    let scalars: Vec<&StoredFunction> = functions.iter().filter(|f| !f.is_table()).collect();
    let tables: Vec<&StoredFunction> = functions.iter().filter(|f| f.is_table()).collect();

    let mut text = sql.to_string();
    for _ in 0..MAX_PASSES {
        let mut changed = false;
        if !tables.is_empty() {
            text = expand_table_funcs(&text, &tables, &mut changed);
        }
        if !scalars.is_empty() {
            text = expand_scalar_funcs(&text, &scalars, &mut changed)?;
        }
        if !changed {
            break;
        }
    }
    Ok(text)
}

// ─────────────────────────────────────────────────────────────────────────────
// Scalar functions — textual call replacement with a scalar subquery
// ─────────────────────────────────────────────────────────────────────────────

/// Replace each `fn(args…)` scalar-function call in `text` with a scalar subquery
/// `(<body with args substituted>)` (CONCEPT:EG-118). Left-to-right, one level per pass
/// (nested calls that end up inside the substituted body are handled by the next pass).
fn expand_scalar_funcs(
    text: &str,
    scalars: &[&StoredFunction],
    changed: &mut bool,
) -> Result<String, String> {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        // Copy through string/dollar/quoted-identifier literals untouched.
        if let Some(end) = skip_literal(text, i) {
            out.push_str(&text[i..end]);
            i = end;
            continue;
        }
        // Try to match a scalar-function call whose name STARTS at `i`.
        if is_ident_start(c) && !preceded_by_qualifier(bytes, i) {
            let name_end = ident_end(bytes, i);
            let name = &text[i..name_end];
            if let Some(f) = scalars
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(name))
                .filter(|_| is_call_open(text, name_end))
            {
                let open = next_non_ws(bytes, name_end);
                if let Some((inner, after)) = read_parens(text, open) {
                    let arg_texts = split_top_commas(inner);
                    let replacement = build_scalar_subquery(f, &arg_texts)?;
                    out.push_str(&replacement);
                    *changed = true;
                    i = after;
                    continue;
                }
            }
            // Not a call we own — copy the whole identifier so we don't rematch inside it.
            out.push_str(name);
            i = name_end;
            continue;
        }
        out.push(c as char);
        i += 1;
    }
    Ok(out)
}

/// Build the scalar subquery `(<substituted body>)` for `f` given the call's argument
/// texts (CONCEPT:EG-118). Errors on an argument-count mismatch.
fn build_scalar_subquery(f: &StoredFunction, arg_texts: &[String]) -> Result<String, String> {
    let subst = substitute_args(&f.body, f, arg_texts)?;
    Ok(format!("({})", subst.trim()))
}

// ─────────────────────────────────────────────────────────────────────────────
// Table functions — AST-level FROM-clause expansion into a derived subquery
// ─────────────────────────────────────────────────────────────────────────────

/// Replace each `FROM fn(args…)` table-function reference with the body `SELECT` as a
/// derived subquery (CONCEPT:EG-118). AST-level so the original alias is honored. On a
/// parse failure the text is returned unchanged (best-effort; DataFusion reports it).
fn expand_table_funcs(text: &str, tables: &[&StoredFunction], changed: &mut bool) -> String {
    let Ok(mut stmts) = Parser::parse_sql(&PostgreSqlDialect {}, text) else {
        return text.to_string();
    };
    let mut local_changed = false;
    for stmt in &mut stmts {
        if let Statement::Query(q) = stmt {
            rewrite_query_from(q, tables, &mut local_changed);
        }
    }
    if local_changed {
        *changed = true;
        stmts
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join("; ")
    } else {
        text.to_string()
    }
}

/// Walk a query's `WITH` CTEs and body, rewriting table-function `FROM` references.
fn rewrite_query_from(q: &mut Query, tables: &[&StoredFunction], changed: &mut bool) {
    if let Some(with) = &mut q.with {
        for cte in &mut with.cte_tables {
            rewrite_query_from(&mut cte.query, tables, changed);
        }
    }
    rewrite_setexpr_from(&mut q.body, tables, changed);
}

/// Rewrite table-function references in a `SetExpr` (a SELECT body or a UNION arm).
fn rewrite_setexpr_from(body: &mut SetExpr, tables: &[&StoredFunction], changed: &mut bool) {
    match body {
        SetExpr::Select(select) => {
            for twj in &mut select.from {
                rewrite_twj_from(twj, tables, changed);
            }
        }
        SetExpr::Query(q) => rewrite_query_from(q, tables, changed),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_setexpr_from(left, tables, changed);
            rewrite_setexpr_from(right, tables, changed);
        }
        _ => {}
    }
}

/// Rewrite the relation + each join relation of a `FROM` item.
fn rewrite_twj_from(twj: &mut TableWithJoins, tables: &[&StoredFunction], changed: &mut bool) {
    rewrite_table_factor(&mut twj.relation, tables, changed);
    for join in &mut twj.joins {
        rewrite_table_factor(&mut join.relation, tables, changed);
    }
}

/// Replace a `TableFactor::Table` that names a table function with a derived subquery,
/// or recurse into a derived/nested-join factor (CONCEPT:EG-118).
fn rewrite_table_factor(tf: &mut TableFactor, tables: &[&StoredFunction], changed: &mut bool) {
    match tf {
        TableFactor::Table {
            name,
            args: Some(fn_args),
            alias,
            ..
        } => {
            let leaf = name.0.last().map(|i| i.value.clone()).unwrap_or_default();
            let Some(f) = tables.iter().find(|f| f.name.eq_ignore_ascii_case(&leaf)) else {
                return;
            };
            // Serialize each call argument to text and substitute into the body.
            let arg_texts: Vec<String> =
                fn_args.args.iter().filter_map(function_arg_text).collect();
            if arg_texts.len() != fn_args.args.len() {
                return; // a `*`/named-arg shape we don't handle — leave it for DataFusion.
            }
            let Ok(subst) = substitute_args(&f.body, f, &arg_texts) else {
                return;
            };
            let Ok(mut parsed) = Parser::parse_sql(&PostgreSqlDialect {}, &subst) else {
                return;
            };
            let subquery = match parsed.as_mut_slice() {
                [Statement::Query(q)] => q.clone(),
                _ => return,
            };
            let alias = alias.clone().or_else(|| {
                Some(TableAlias {
                    name: Ident::new(f.name.clone()),
                    columns: Vec::new(),
                })
            });
            *tf = TableFactor::Derived {
                lateral: false,
                subquery,
                alias,
            };
            *changed = true;
        }
        TableFactor::Derived { subquery, .. } => {
            rewrite_query_from(subquery, tables, changed);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => rewrite_twj_from(table_with_joins, tables, changed),
        _ => {}
    }
}

/// Serialize one table-function call argument to SQL text, or `None` for a shape we do
/// not substitute (a `*`/qualified-`*`/named arg).
fn function_arg_text(arg: &FunctionArg) -> Option<String> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(e),
            ..
        } => Some(e.to_string()),
        _ => None,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Argument substitution (shared by scalar + table expansion)
// ─────────────────────────────────────────────────────────────────────────────

/// Substitute the argument identifiers in `body` with the call's argument texts
/// (CONCEPT:EG-118). Whole-word + quote-aware: an argument name inside a string literal,
/// dollar body, or quoted identifier — or a qualified `x.arg` reference — is NOT
/// replaced. Each substitution is PARENTHESIZED so operator precedence is preserved.
/// Errors if the number of call arguments differs from the declared arity.
fn substitute_args(body: &str, f: &StoredFunction, arg_texts: &[String]) -> Result<String, String> {
    if arg_texts.len() != f.args.len() {
        return Err(format!(
            "function `{}` expects {} argument(s), got {}",
            f.name,
            f.args.len(),
            arg_texts.len()
        ));
    }
    if f.args.is_empty() {
        return Ok(body.to_string());
    }
    let bytes = body.as_bytes();
    let mut out = String::with_capacity(body.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(end) = skip_literal(body, i) {
            out.push_str(&body[i..end]);
            i = end;
            continue;
        }
        let c = bytes[i];
        if is_ident_start(c) {
            let end = ident_end(bytes, i);
            let word = &body[i..end];
            // A qualified reference (`t.arg`) keeps its column meaning — do not rewrite.
            let qualified = i > 0 && bytes[i - 1] == b'.';
            if !qualified {
                if let Some(idx) = f
                    .args
                    .iter()
                    .position(|a| a.name.eq_ignore_ascii_case(word))
                {
                    out.push('(');
                    out.push_str(arg_texts[idx].trim());
                    out.push(')');
                    i = end;
                    continue;
                }
            }
            out.push_str(word);
            i = end;
            continue;
        }
        out.push(c as char);
        i += 1;
    }
    Ok(out)
}

// ─────────────────────────────────────────────────────────────────────────────
// Small lexical helpers (byte-oriented; ASCII SQL punctuation)
// ─────────────────────────────────────────────────────────────────────────────

/// If a string/dollar/quoted-identifier literal STARTS at byte `i`, return the byte
/// index just past it; else `None`. Keeps a function/argument name inside a literal from
/// being matched (CONCEPT:EG-118).
fn skip_literal(s: &str, i: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    match bytes.get(i)? {
        b'\'' => Some(skip_single_quote(bytes, i)),
        b'"' => Some(skip_delim(bytes, i, b'"')),
        b'$' => super::pgfamily::read_dollar_quoted(s, i).map(|(_, end)| end),
        _ => None,
    }
}

/// Index past a `'…'` string (Postgres `''` escape).
fn skip_single_quote(bytes: &[u8], start: usize) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if bytes.get(i + 1) == Some(&b'\'') {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    bytes.len()
}

/// Index past a `<delim>…<delim>` run (e.g. a `"quoted identifier"`).
fn skip_delim(bytes: &[u8], start: usize, delim: u8) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == delim {
            return i + 1;
        }
        i += 1;
    }
    bytes.len()
}

/// Whether `b` may start a SQL identifier.
fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

/// Whether `b` may continue a SQL identifier.
fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// The byte index just past the identifier starting at `start`.
fn ident_end(bytes: &[u8], start: usize) -> usize {
    let mut i = start + 1;
    while i < bytes.len() && is_ident_byte(bytes[i]) {
        i += 1;
    }
    i
}

/// Whether the identifier at `i` is preceded by a `.` qualifier (`schema.fn`) — such a
/// call is left alone (we only expand bare, unqualified stored-function names).
fn preceded_by_qualifier(bytes: &[u8], i: usize) -> bool {
    i > 0 && bytes[i - 1] == b'.'
}

/// Whether, skipping whitespace from `pos`, the next byte is a call-opening `(`.
fn is_call_open(s: &str, pos: usize) -> bool {
    let bytes = s.as_bytes();
    s.as_bytes().get(next_non_ws(bytes, pos)) == Some(&b'(')
}

/// The byte index of the next non-whitespace byte at/after `pos`.
fn next_non_ws(bytes: &[u8], pos: usize) -> usize {
    let mut i = pos;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

/// Read a balanced `(…)` whose `(` is at byte `open`. Returns the INNER text and the
/// byte index just past the matching `)`. Skips string/dollar/quoted literals.
fn read_parens(s: &str, open: usize) -> Option<(&str, usize)> {
    let bytes = s.as_bytes();
    if *bytes.get(open)? != b'(' {
        return None;
    }
    let mut depth = 0usize;
    let mut i = open;
    while i < bytes.len() {
        if let Some(end) = skip_literal(s, i) {
            i = end;
            continue;
        }
        match bytes[i] {
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

/// Split `inner` at top-level commas (not nested in parens/strings/dollar bodies),
/// trimming each piece. An empty (or whitespace-only) `inner` ⇒ no arguments.
fn split_top_commas(inner: &str) -> Vec<String> {
    if inner.trim().is_empty() {
        return Vec::new();
    }
    let bytes = inner.as_bytes();
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(end) = skip_literal(inner, i) {
            i = end;
            continue;
        }
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                out.push(inner[start..i].trim().to_string());
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    out.push(inner[start..].trim().to_string());
    out
}
