//! PL/pgSQL procedural interpreter (CONCEPT:EG-KG.query.eg-validate-procedural-body / EG-341).
//!
//! `CREATE FUNCTION … LANGUAGE plpgsql AS $$ … $$` stores a *procedural* body (not a
//! single `SELECT`), so it cannot be inlined like a `LANGUAGE sql` body (CONCEPT:EG-KG.query.create-drop-function).
//! Instead, when a **bare top-level** `SELECT fn(args)` or `CALL fn(args)` names a
//! plpgsql function, [`try_exec_call`] runs a small hand-written statement interpreter
//! over the parsed body against a variable environment. Every value-producing operation
//! — expression evaluation, `SELECT … INTO`, a loop bound — is executed as embedded SQL
//! through the SAME read path the caller passes in (`run_sql`), so the engine's real SQL
//! planner does all arithmetic/comparison/function work and there is no second expression
//! evaluator to keep in sync.
//!
//! ## Implemented subset (CONCEPT:EG-KG.query.concept-7)
//! * `DECLARE` variables (`name type [:= init]`) at the function's top-level block.
//! * `BEGIN … END` (including a nested `BEGIN … END;` block).
//! * Assignment `var := <expr>`.
//! * `IF … THEN … [ELSIF … THEN …] [ELSE …] END IF`.
//! * `LOOP … END LOOP`, `WHILE <cond> LOOP … END LOOP`,
//!   `FOR v IN [REVERSE] lo..hi [BY step] LOOP … END LOOP` (integer range).
//! * `EXIT [WHEN <cond>]`, `CONTINUE [WHEN <cond>]`.
//! * `RETURN [<expr>]`.
//! * `RAISE [EXCEPTION|NOTICE|WARNING|…] '<message>'` (EXCEPTION aborts with the message;
//!   lesser levels are logged and ignored).
//! * Embedded SQL: `SELECT <exprs> INTO <vars> [FROM …]` binds result columns to
//!   variables; a bare `PERFORM <expr>` / `SELECT …` runs and discards its result.
//! * A variable reference inside any embedded SQL is substituted as a SQL literal
//!   (quote-aware; a qualified `t.col` is left alone).
//!
//! ## Out of scope (documented follow-ups)
//! Set-returning `RETURN NEXT` / `RETURN QUERY`, `FOR row IN <query> LOOP`, cursors,
//! `RECORD`/`%ROWTYPE`/`%TYPE` composite variables, exception handlers
//! (`BEGIN … EXCEPTION WHEN …`), dynamic `EXECUTE`, `GET DIAGNOSTICS`, `OUT`/`INOUT`
//! parameters, `RAISE` format arguments (`%`), and DML inside a body (the interception
//! runs on the read path). An embedded call to another plpgsql function only resolves in
//! the *bare* `SELECT fn(x)` form, not nested inside a larger expression.

use std::collections::HashMap;

use super::exec::{PgColType, TypedColumn, TypedQueryResult};
use crate::tables::schema::StoredFunction;

/// Hard cap on total interpreted loop iterations, guarding a runaway `LOOP`/`WHILE`
/// against hanging the reactor's blocking pool (CONCEPT:EG-KG.query.concept-7).
const MAX_STEPS: u64 = 5_000_000;

// ─────────────────────────────────────────────────────────────────────────────
// Values
// ─────────────────────────────────────────────────────────────────────────────

/// A runtime scalar value held in the variable environment (CONCEPT:EG-KG.query.concept-7). Decoded
/// from an embedded-SQL result cell and re-encoded as a SQL literal for substitution.
#[derive(Clone, Debug)]
enum Val {
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Null,
}

impl Val {
    /// Decode a JSON result cell (the shape [`TypedQueryResult`] rows carry) into a value.
    fn from_json(v: &serde_json::Value) -> Val {
        match v {
            serde_json::Value::Null => Val::Null,
            serde_json::Value::Bool(b) => Val::Bool(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Val::Int(i)
                } else if let Some(u) = n.as_u64() {
                    Val::Int(u as i64)
                } else {
                    Val::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::String(s) => Val::Text(s.clone()),
            other => Val::Text(other.to_string()),
        }
    }

    /// The value as a JSON cell for a [`TypedQueryResult`] row.
    fn to_json(&self) -> serde_json::Value {
        match self {
            Val::Int(i) => serde_json::json!(i),
            Val::Float(f) => serde_json::json!(f),
            Val::Text(s) => serde_json::json!(s),
            Val::Bool(b) => serde_json::json!(b),
            Val::Null => serde_json::Value::Null,
        }
    }

    /// The value as a SQL literal to splice into embedded SQL (quote-safe).
    fn to_sql_literal(&self) -> String {
        match self {
            Val::Int(i) => i.to_string(),
            Val::Float(f) => {
                if f.is_finite() {
                    format!("{f}")
                } else {
                    "NULL".to_string()
                }
            }
            Val::Text(s) => format!("'{}'", s.replace('\'', "''")),
            Val::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            Val::Null => "NULL".to_string(),
        }
    }

    /// Truthiness for a condition (`Bool`, or a non-zero number).
    fn as_bool(&self) -> Result<bool, String> {
        match self {
            Val::Bool(b) => Ok(*b),
            Val::Int(i) => Ok(*i != 0),
            Val::Float(f) => Ok(*f != 0.0),
            Val::Null => Ok(false),
            Val::Text(s) => match s.to_ascii_lowercase().as_str() {
                "t" | "true" | "yes" | "on" | "1" => Ok(true),
                "f" | "false" | "no" | "off" | "0" => Ok(false),
                _ => Err(format!("condition is not boolean: '{s}'")),
            },
        }
    }

    /// The value as an `i64` (for a FOR-loop bound/step).
    fn as_int(&self) -> Result<i64, String> {
        match self {
            Val::Int(i) => Ok(*i),
            Val::Float(f) => Ok(*f as i64),
            Val::Bool(b) => Ok(*b as i64),
            Val::Text(s) => s
                .trim()
                .parse::<i64>()
                .map_err(|_| format!("expected an integer, got '{s}'")),
            Val::Null => Err("expected an integer, got NULL".to_string()),
        }
    }

    /// The Postgres-mappable column type this value reports in a scalar result.
    fn col_type(&self) -> PgColType {
        match self {
            Val::Int(_) => PgColType::Int8,
            Val::Float(_) => PgColType::Float8,
            Val::Bool(_) => PgColType::Bool,
            Val::Text(_) | Val::Null => PgColType::Text,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// AST
// ─────────────────────────────────────────────────────────────────────────────

/// One top-level `DECLARE` variable: `name [type] [:= init]` (CONCEPT:EG-KG.query.concept-7). The type
/// spelling is catalog metadata only — values are typed by the SQL that produces them.
#[derive(Debug, Clone)]
struct Decl {
    name: String,
    init: Option<String>,
}

/// A parsed procedural statement (CONCEPT:EG-KG.query.concept-7). Expression/SQL text is kept as the raw
/// source slice so it substitutes + runs through the real SQL planner verbatim.
#[derive(Debug, Clone)]
enum Stmt {
    Assign {
        var: String,
        expr: String,
    },
    Return(Option<String>),
    If {
        arms: Vec<(String, Vec<Stmt>)>,
        els: Option<Vec<Stmt>>,
    },
    While {
        cond: String,
        body: Vec<Stmt>,
    },
    Loop {
        body: Vec<Stmt>,
    },
    For {
        var: String,
        reverse: bool,
        lo: String,
        hi: String,
        step: Option<String>,
        body: Vec<Stmt>,
    },
    Exit {
        when: Option<String>,
    },
    Continue {
        when: Option<String>,
    },
    Raise {
        fatal: bool,
        message: Option<String>,
    },
    /// `SELECT <select_sql> INTO <vars>` — run `select_sql` and bind result columns.
    SelectInto {
        vars: Vec<String>,
        select_sql: String,
    },
    /// A bare `PERFORM`/`SELECT` statement whose result is discarded.
    Perform(String),
    /// A nested `BEGIN … END` block (no sub-`DECLARE`/`EXCEPTION`).
    Block(Vec<Stmt>),
    /// `NULL;` — a no-op placeholder statement.
    Noop,
}

/// A fully parsed PL/pgSQL body: its declared variables and top-level statements
/// (CONCEPT:EG-KG.query.concept-7).
#[derive(Debug, Clone)]
pub(super) struct PlBody {
    decls: Vec<Decl>,
    body: Vec<Stmt>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Tokenizer
// ─────────────────────────────────────────────────────────────────────────────

/// One lexical token with its byte span into the original source, so a captured
/// expression is sliced verbatim (preserving spacing / `t.col` / string literals).
#[derive(Debug, Clone)]
struct Tok {
    text: String,
    start: usize,
    end: usize,
}

impl Tok {
    fn is(&self, kw: &str) -> bool {
        self.text.eq_ignore_ascii_case(kw)
    }
}

/// Tokenize a plpgsql body: identifiers, numbers, `'…'` strings (kept whole), the
/// multi-char operators `:=` and `..`, and single punctuation chars. `--` line comments
/// and `/* … */` block comments are skipped.
/// Skip whitespace or a `--`/`/* */` comment starting at `i`. Split out of
/// `tokenize` (extract-method, cx/wD8) — same terms, same order as before.
/// Returns the index to resume at, or `None` if `b[i]` is neither.
/// Skip a `--` line comment starting at `i` (`b[i] == '-'`). Split out of
/// `skip_tokenize_trivia` (extract-method, cx/wD8) — same terms, same order
/// as before.
fn skip_line_comment(b: &[u8], i: usize) -> usize {
    let mut j = i;
    while j < b.len() && b[j] != b'\n' {
        j += 1;
    }
    j
}

/// Skip a `/* */` block comment starting at `i` (`b[i] == '/'`). Split out
/// of `skip_tokenize_trivia` (extract-method, cx/wD8) — same terms, same
/// order as before.
fn skip_block_comment(b: &[u8], i: usize) -> usize {
    let mut j = i + 2;
    while j < b.len() && !(b[j] == b'*' && b.get(j + 1) == Some(&b'/')) {
        j += 1;
    }
    (j + 2).min(b.len())
}

fn skip_tokenize_trivia(b: &[u8], i: usize) -> Option<usize> {
    let c = b[i];
    if c.is_ascii_whitespace() {
        return Some(i + 1);
    }
    if c == b'-' && b.get(i + 1) == Some(&b'-') {
        return Some(skip_line_comment(b, i));
    }
    if c == b'/' && b.get(i + 1) == Some(&b'*') {
        return Some(skip_block_comment(b, i));
    }
    None
}

/// Tokenize a single-quoted string literal (Postgres `''` escape) starting
/// at `i`, kept as one token including quotes. Split out of `tokenize`
/// (extract-method, cx/wD8) — same terms, same order as before. `Ok(None)`
/// means `b[i]` isn't a `'`.
fn tokenize_string_literal(
    src: &str,
    b: &[u8],
    i: usize,
) -> Result<Option<(Tok, usize)>, String> {
    if b[i] != b'\'' {
        return Ok(None);
    }
    let start = i;
    let mut j = i + 1;
    loop {
        if j >= b.len() {
            return Err("unterminated string literal in plpgsql body".to_string());
        }
        if b[j] == b'\'' {
            if b.get(j + 1) == Some(&b'\'') {
                j += 2;
                continue;
            }
            j += 1;
            break;
        }
        j += 1;
    }
    Ok(Some((
        Tok {
            text: src[start..j].to_string(),
            start,
            end: j,
        },
        j,
    )))
}

/// Tokenize an identifier/keyword starting at `i`. Split out of `tokenize`
/// (extract-method, cx/wD8) — same terms, same order as before.
fn tokenize_identifier(src: &str, b: &[u8], i: usize) -> Option<(Tok, usize)> {
    let c = b[i];
    if !(c.is_ascii_alphabetic() || c == b'_') {
        return None;
    }
    let start = i;
    let mut j = i + 1;
    while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
        j += 1;
    }
    Some((
        Tok {
            text: src[start..j].to_string(),
            start,
            end: j,
        },
        j,
    ))
}

/// Tokenize an integer/float literal starting at `i` (a leading-dot number
/// is rare in bodies, handled as punct). Split out of `tokenize`
/// (extract-method, cx/wD8) — same terms, same order as before.
fn tokenize_number(src: &str, b: &[u8], i: usize) -> Option<(Tok, usize)> {
    let c = b[i];
    if !c.is_ascii_digit() {
        return None;
    }
    let start = i;
    let mut j = i + 1;
    while j < b.len() && (b[j].is_ascii_digit() || b[j] == b'.') {
        // Stop before the `..` range operator.
        if b[j] == b'.' && b.get(j + 1) == Some(&b'.') {
            break;
        }
        j += 1;
    }
    Some((
        Tok {
            text: src[start..j].to_string(),
            start,
            end: j,
        },
        j,
    ))
}

/// Tokenize the `:=` or `..` multi-char operators starting at `i`. Split out
/// of `tokenize` (extract-method, cx/wD8) — same terms, same order as
/// before.
fn tokenize_multichar_operator(b: &[u8], i: usize) -> Option<(Tok, usize)> {
    let c = b[i];
    if c == b':' && b.get(i + 1) == Some(&b'=') {
        return Some((
            Tok {
                text: ":=".to_string(),
                start: i,
                end: i + 2,
            },
            i + 2,
        ));
    }
    if c == b'.' && b.get(i + 1) == Some(&b'.') {
        return Some((
            Tok {
                text: "..".to_string(),
                start: i,
                end: i + 2,
            },
            i + 2,
        ));
    }
    None
}

fn tokenize(src: &str) -> Result<Vec<Tok>, String> {
    let b = src.as_bytes();
    let mut toks = Vec::new();
    let mut i = 0usize;
    while i < b.len() {
        if let Some(next) = skip_tokenize_trivia(b, i) {
            i = next;
            continue;
        }
        if let Some((tok, next)) = tokenize_string_literal(src, b, i)? {
            toks.push(tok);
            i = next;
            continue;
        }
        if let Some((tok, next)) = tokenize_identifier(src, b, i) {
            toks.push(tok);
            i = next;
            continue;
        }
        if let Some((tok, next)) = tokenize_number(src, b, i) {
            toks.push(tok);
            i = next;
            continue;
        }
        if let Some((tok, next)) = tokenize_multichar_operator(b, i) {
            toks.push(tok);
            i = next;
            continue;
        }
        // Any other single punctuation char.
        let c = b[i];
        toks.push(Tok {
            text: (c as char).to_string(),
            start: i,
            end: i + 1,
        });
        i += 1;
    }
    Ok(toks)
}

// ─────────────────────────────────────────────────────────────────────────────
// Parser
// ─────────────────────────────────────────────────────────────────────────────

struct Parser<'a> {
    src: &'a str,
    toks: Vec<Tok>,
    pos: usize,
}

/// Parse a `LANGUAGE plpgsql` body into a [`PlBody`] (CONCEPT:EG-KG.query.concept-7). Public so
/// `CREATE FUNCTION` can validate the body up front.
pub(super) fn parse_body(src: &str) -> Result<PlBody, String> {
    let toks = tokenize(src)?;
    let mut p = Parser { src, toks, pos: 0 };
    let body = p.parse_body()?;
    Ok(body)
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn peek_is(&self, kw: &str) -> bool {
        self.peek().is_some_and(|t| t.is(kw))
    }

    fn advance(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        if self.peek_is(kw) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect_kw(&mut self, kw: &str) -> Result<(), String> {
        if self.eat_kw(kw) {
            Ok(())
        } else {
            Err(format!(
                "expected `{kw}`, found {}",
                self.peek()
                    .map(|t| t.text.as_str())
                    .unwrap_or("end of body")
            ))
        }
    }

    fn expect_ident(&mut self) -> Result<String, String> {
        match self.advance() {
            Some(t)
                if t.text
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_alphabetic() || c == '_') =>
            {
                Ok(t.text)
            }
            Some(t) => Err(format!("expected an identifier, found `{}`", t.text)),
            None => Err("expected an identifier, found end of body".to_string()),
        }
    }

    /// The full body: optional `DECLARE … ` then `BEGIN … END [label] [;]`.
    fn parse_body(&mut self) -> Result<PlBody, String> {
        let mut decls = Vec::new();
        if self.eat_kw("declare") {
            while !self.peek_is("begin") {
                if self.peek().is_none() {
                    return Err("plpgsql DECLARE section is missing its `BEGIN`".to_string());
                }
                decls.push(self.parse_decl()?);
            }
        }
        self.expect_kw("begin")?;
        let body = self.parse_stmt_list()?;
        self.expect_kw("end")?;
        // Optional end-label (an identifier that is not `;`).
        if let Some(t) = self.peek() {
            if t.text != ";" {
                self.pos += 1;
            }
        }
        self.eat_kw(";");
        if self.peek().is_some() {
            return Err(format!(
                "trailing tokens after the final `END` (found `{}`)",
                self.peek().map(|t| t.text.as_str()).unwrap_or("")
            ));
        }
        Ok(PlBody { decls, body })
    }

    /// One `DECLARE` line: `name [type words…] [:= <init>] ;`.
    fn parse_decl(&mut self) -> Result<Decl, String> {
        let name = self.expect_ident()?;
        let mut init = None;
        // Skip the type spelling up to `:=` or `;`.
        while let Some(t) = self.peek() {
            if t.text == ";" {
                break;
            }
            if t.text == ":=" || t.is("default") {
                self.pos += 1;
                init = Some(self.capture_until(&[";"])?);
                break;
            }
            self.pos += 1;
        }
        self.expect_kw(";")?;
        Ok(Decl { name, init })
    }

    /// Parse statements until a block terminator keyword (`END`/`ELSIF`/`ELSE`) or EOF.
    fn parse_stmt_list(&mut self) -> Result<Vec<Stmt>, String> {
        let mut out = Vec::new();
        loop {
            match self.peek() {
                None => break,
                Some(t) if t.is("end") || t.is("elsif") || t.is("else") => break,
                _ => out.push(self.parse_stmt()?),
            }
        }
        Ok(out)
    }

    /// `WHILE cond LOOP body END LOOP ;`. Split out of `parse_stmt`
    /// (extract-method, cx/wD8) — same terms, same order as before.
    fn parse_while_stmt(&mut self) -> Result<Stmt, String> {
        self.pos += 1;
        let cond = self.capture_until(&["loop"])?;
        self.expect_kw("loop")?;
        let body = self.parse_stmt_list()?;
        self.expect_kw("end")?;
        self.expect_kw("loop")?;
        self.eat_kw(";");
        Ok(Stmt::While { cond, body })
    }

    /// `LOOP body END LOOP ;`. Split out of `parse_stmt` (extract-method,
    /// cx/wD8) — same terms, same order as before.
    fn parse_bare_loop_stmt(&mut self) -> Result<Stmt, String> {
        self.pos += 1;
        let body = self.parse_stmt_list()?;
        self.expect_kw("end")?;
        self.expect_kw("loop")?;
        self.eat_kw(";");
        Ok(Stmt::Loop { body })
    }

    /// `RETURN [expr] ;`. Split out of `parse_stmt` (extract-method,
    /// cx/wD8) — same terms, same order as before.
    fn parse_return_stmt(&mut self) -> Result<Stmt, String> {
        self.pos += 1;
        if self.eat_kw(";") {
            return Ok(Stmt::Return(None));
        }
        if self.peek_is("next") || self.peek_is("query") {
            return Err(
                "RETURN NEXT/QUERY (set-returning plpgsql) is out of scope (CONCEPT:EG-KG.query.concept-7)"
                    .to_string(),
            );
        }
        let e = self.capture_until(&[";"])?;
        self.expect_kw(";")?;
        Ok(Stmt::Return(Some(e)))
    }

    /// `EXIT [WHEN cond] ;`. Split out of `parse_stmt` (extract-method,
    /// cx/wD8) — same terms, same order as before.
    fn parse_exit_stmt(&mut self) -> Result<Stmt, String> {
        self.pos += 1;
        let when = self.parse_optional_when()?;
        self.expect_kw(";")?;
        Ok(Stmt::Exit { when })
    }

    /// `CONTINUE [WHEN cond] ;`. Split out of `parse_stmt` (extract-method,
    /// cx/wD8) — same terms, same order as before.
    fn parse_continue_stmt(&mut self) -> Result<Stmt, String> {
        self.pos += 1;
        let when = self.parse_optional_when()?;
        self.expect_kw(";")?;
        Ok(Stmt::Continue { when })
    }

    /// `BEGIN body END ;`. Split out of `parse_stmt` (extract-method,
    /// cx/wD8) — same terms, same order as before.
    fn parse_begin_block_stmt(&mut self) -> Result<Stmt, String> {
        self.pos += 1;
        let body = self.parse_stmt_list()?;
        if self.peek_is("exception") {
            return Err(
                "BEGIN … EXCEPTION handlers are out of scope (CONCEPT:EG-KG.query.concept-7)"
                    .to_string(),
            );
        }
        self.expect_kw("end")?;
        self.eat_kw(";");
        Ok(Stmt::Block(body))
    }

    /// `PERFORM expr ;`. Split out of `parse_stmt` (extract-method,
    /// cx/wD8) — same terms, same order as before.
    fn parse_perform_stmt(&mut self) -> Result<Stmt, String> {
        self.pos += 1;
        let e = self.capture_until(&[";"])?;
        self.expect_kw(";")?;
        Ok(Stmt::Perform(format!("SELECT {e}")))
    }

    /// `ident := expr ;`. Split out of `parse_stmt` (extract-method,
    /// cx/wD8) — same terms, same order as before.
    fn parse_assignment_stmt(&mut self) -> Result<Stmt, String> {
        let var = self.advance().unwrap().text;
        self.expect_kw(":=")?;
        let expr = self.capture_until(&[";"])?;
        self.expect_kw(";")?;
        Ok(Stmt::Assign { var, expr })
    }

    /// Any other embedded SQL verb we can run and discard (`INSERT`/`UPDATE`/
    /// `DELETE`/`CALL`). Split out of `parse_stmt` (extract-method,
    /// cx/wD8) — same terms, same order as before.
    fn parse_passthrough_sql_stmt(&mut self) -> Result<Stmt, String> {
        let sql = self.capture_until(&[";"])?;
        self.expect_kw(";")?;
        Ok(Stmt::Perform(sql))
    }

    fn parse_stmt(&mut self) -> Result<Stmt, String> {
        let t = self.peek().ok_or("unexpected end of body")?.clone();
        if t.is("if") {
            return self.parse_if();
        }
        if t.is("while") {
            return self.parse_while_stmt();
        }
        if t.is("loop") {
            return self.parse_bare_loop_stmt();
        }
        if t.is("for") {
            return self.parse_for();
        }
        if t.is("return") {
            return self.parse_return_stmt();
        }
        if t.is("exit") {
            return self.parse_exit_stmt();
        }
        self.parse_stmt_kw_tail(&t)
    }

    /// The middle of `parse_stmt`'s dispatch: `CONTINUE`, `RAISE`, `BEGIN`,
    /// `PERFORM`. Split out of `parse_stmt` (extract-method, cx/wD8) — same
    /// terms, same order as before.
    fn parse_stmt_kw_tail(&mut self, t: &Tok) -> Result<Stmt, String> {
        if t.is("continue") {
            return self.parse_continue_stmt();
        }
        if t.is("raise") {
            return self.parse_raise();
        }
        if t.is("begin") {
            return self.parse_begin_block_stmt();
        }
        if t.is("perform") {
            return self.parse_perform_stmt();
        }
        self.parse_stmt_expr_tail(t)
    }

    /// The tail of `parse_stmt`'s dispatch: `NULL ;`, `SELECT`,
    /// assignment, a passthrough SQL verb, or the unsupported-statement
    /// error. Split out of `parse_stmt_kw_tail` (extract-method, cx/wD8) —
    /// same terms, same order as before.
    fn parse_stmt_expr_tail(&mut self, t: &Tok) -> Result<Stmt, String> {
        if t.is("null") && self.toks.get(self.pos + 1).is_some_and(|n| n.text == ";") {
            self.pos += 2;
            return Ok(Stmt::Noop);
        }
        if t.is("select") {
            return self.parse_select_stmt();
        }
        // Assignment: `ident := expr ;`.
        let is_assign = (t
            .text
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_'))
            && self.toks.get(self.pos + 1).is_some_and(|n| n.text == ":=");
        if is_assign {
            return self.parse_assignment_stmt();
        }
        // Any other embedded SQL verb we can run and discard.
        if t.is("insert") || t.is("update") || t.is("delete") || t.is("call") {
            return self.parse_passthrough_sql_stmt();
        }
        Err(format!(
            "unsupported plpgsql statement starting at `{}` (CONCEPT:EG-KG.query.concept-7)",
            t.text
        ))
    }

    fn parse_if(&mut self) -> Result<Stmt, String> {
        self.expect_kw("if")?;
        let mut arms = Vec::new();
        let cond = self.capture_until(&["then"])?;
        self.expect_kw("then")?;
        arms.push((cond, self.parse_stmt_list()?));
        while self.eat_kw("elsif") {
            let c = self.capture_until(&["then"])?;
            self.expect_kw("then")?;
            arms.push((c, self.parse_stmt_list()?));
        }
        let els = if self.eat_kw("else") {
            Some(self.parse_stmt_list()?)
        } else {
            None
        };
        self.expect_kw("end")?;
        self.expect_kw("if")?;
        self.eat_kw(";");
        Ok(Stmt::If { arms, els })
    }

    fn parse_for(&mut self) -> Result<Stmt, String> {
        self.expect_kw("for")?;
        let var = self.expect_ident()?;
        self.expect_kw("in")?;
        let reverse = self.eat_kw("reverse");
        let lo = self.capture_until(&[".."])?;
        self.expect_kw("..")?;
        let hi = self.capture_until(&["loop", "by"])?;
        let step = if self.eat_kw("by") {
            Some(self.capture_until(&["loop"])?)
        } else {
            None
        };
        self.expect_kw("loop")?;
        let body = self.parse_stmt_list()?;
        self.expect_kw("end")?;
        self.expect_kw("loop")?;
        self.eat_kw(";");
        Ok(Stmt::For {
            var,
            reverse,
            lo,
            hi,
            step,
            body,
        })
    }

    fn parse_raise(&mut self) -> Result<Stmt, String> {
        self.expect_kw("raise")?;
        // Optional level word.
        let levels = ["debug", "log", "info", "notice", "warning", "exception"];
        let mut fatal = true; // a bare `RAISE 'msg'` behaves like EXCEPTION here.
        if let Some(t) = self.peek() {
            if levels.iter().any(|l| t.is(l)) {
                fatal = t.is("exception");
                self.pos += 1;
            }
        }
        // Optional message string literal; skip any format args up to `;`.
        let mut message = None;
        if let Some(t) = self.peek() {
            if t.text.starts_with('\'') {
                message = Some(unquote(&t.text));
                self.pos += 1;
            }
        }
        while let Some(t) = self.peek() {
            if t.text == ";" {
                break;
            }
            self.pos += 1;
        }
        self.expect_kw(";")?;
        Ok(Stmt::Raise { fatal, message })
    }

    fn parse_optional_when(&mut self) -> Result<Option<String>, String> {
        if self.eat_kw("when") {
            Ok(Some(self.capture_until(&[";"])?))
        } else {
            Ok(None)
        }
    }

    /// A `SELECT … [INTO vars] …;` embedded statement. If a top-level `INTO` is present,
    /// its variable list is extracted and the `INTO vars` span removed to form a plain
    /// SELECT bound to the variables; otherwise the whole SELECT runs and is discarded.
    fn parse_select_stmt(&mut self) -> Result<Stmt, String> {
        let start = self.peek().unwrap().start;
        // Find the terminating top-level `;`.
        let mut into_at: Option<usize> = None; // token index of INTO
        while let Some(t) = self.peek() {
            if t.text == ";" {
                break;
            }
            if t.is("into") && into_at.is_none() {
                into_at = Some(self.pos);
            }
            self.pos += 1;
        }
        let end = self
            .peek()
            .ok_or("SELECT statement is missing its terminating `;`")?
            .start;
        let full = self.src[start..end].to_string();
        let result = if let Some(into_idx) = into_at {
            let (vars, into_start, into_end) = self.parse_select_into_vars(into_idx)?;
            // Rebuild the SELECT with the `INTO vars` span cut out.
            let mut select_sql = String::new();
            select_sql.push_str(self.src[start..into_start].trim_end());
            select_sql.push(' ');
            select_sql.push_str(self.src[into_end..end].trim_start());
            Stmt::SelectInto {
                vars,
                select_sql: select_sql.trim().to_string(),
            }
        } else {
            Stmt::Perform(full)
        };
        self.expect_kw(";")?;
        Ok(result)
    }

    /// Parse the `INTO var, var, ...` target-variable list starting right
    /// after the `INTO` token at `into_idx`. Split out of `parse_select_stmt`
    /// (extract-method, cx/wD8) — same terms, same order as before. Returns
    /// (vars, into_start, into_end) — the byte span of `INTO vars` to cut
    /// out of the reconstructed SELECT text.
    fn parse_select_into_vars(&self, into_idx: usize) -> Result<(Vec<String>, usize, usize), String> {
        let mut vars = Vec::new();
        let mut j = into_idx + 1;
        let into_start = self.toks[into_idx].start;
        let mut into_end = self.toks[into_idx].end;
        let clause_kw = [
            "from", "where", "group", "order", "limit", "having", "union",
        ];
        while j < self.pos {
            let tk = &self.toks[j];
            if tk.text == ";" || clause_kw.iter().any(|k| tk.is(k)) {
                break;
            }
            if tk.text == "," {
                into_end = tk.end;
                j += 1;
                continue;
            }
            if tk
                .text
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            {
                vars.push(tk.text.clone());
                into_end = tk.end;
                j += 1;
            } else {
                break;
            }
        }
        if vars.is_empty() {
            return Err("SELECT … INTO requires at least one target variable".to_string());
        }
        Ok((vars, into_start, into_end))
    }

    /// Capture the raw source slice of the tokens from the cursor up to (but not
    /// consuming) the first top-level token whose text matches one of `stops` (a keyword
    /// or a punctuation string). Parenthesis-depth aware so a `,`/keyword inside `(...)`
    /// does not stop capture. `;` always stops. Errors if EOF is reached first.
    fn capture_until(&mut self, stops: &[&str]) -> Result<String, String> {
        let start_tok = self.pos;
        let mut depth = 0i32;
        while let Some(t) = self.peek() {
            if depth == 0 {
                if t.text == ";" && !stops.contains(&";") {
                    break;
                }
                if stops.iter().any(|s| t.is(s) || t.text == *s) {
                    break;
                }
            }
            match t.text.as_str() {
                "(" => depth += 1,
                ")" => depth = (depth - 1).max(0),
                _ => {}
            }
            self.pos += 1;
        }
        if self.pos == start_tok {
            return Err(format!(
                "empty expression before `{}`",
                self.peek()
                    .map(|t| t.text.as_str())
                    .unwrap_or("end of body")
            ));
        }
        let s = self.toks[start_tok].start;
        let e = self.toks[self.pos - 1].end;
        Ok(self.src[s..e].trim().to_string())
    }
}

/// Strip the surrounding single quotes off a string-literal token and un-double `''`.
fn unquote(tok: &str) -> String {
    let inner = tok.strip_prefix('\'').unwrap_or(tok);
    let inner = inner.strip_suffix('\'').unwrap_or(inner);
    inner.replace("''", "'")
}

// ─────────────────────────────────────────────────────────────────────────────
// Interpreter
// ─────────────────────────────────────────────────────────────────────────────

/// Non-local control flow produced by executing a statement/block (CONCEPT:EG-KG.query.concept-7).
enum Flow {
    Normal,
    Return(Val),
    Exit,
    Continue,
}

struct Interp<'a> {
    env: HashMap<String, Val>,
    run_sql: &'a dyn Fn(&str) -> Result<TypedQueryResult, String>,
    steps: u64,
}

impl<'a> Interp<'a> {
    fn tick(&mut self) -> Result<(), String> {
        self.steps += 1;
        if self.steps > MAX_STEPS {
            return Err(format!(
                "plpgsql exceeded {MAX_STEPS} interpreted steps — aborting a probable infinite loop"
            ));
        }
        Ok(())
    }

    /// Run one embedded SQL query (already variable-substituted) and return its rows.
    fn query(&self, sql: &str) -> Result<TypedQueryResult, String> {
        (self.run_sql)(sql)
    }

    /// Evaluate an expression to a scalar by running `SELECT (<expr>)`.
    fn eval(&self, expr: &str) -> Result<Val, String> {
        let sql = format!("SELECT ({})", substitute_vars(expr, &self.env));
        let res = self.query(&sql)?;
        Ok(res
            .rows
            .first()
            .and_then(|r| r.first())
            .map(Val::from_json)
            .unwrap_or(Val::Null))
    }

    fn eval_bool(&self, expr: &str) -> Result<bool, String> {
        self.eval(expr)?.as_bool()
    }

    fn exec_list(&mut self, stmts: &[Stmt]) -> Result<Flow, String> {
        for s in stmts {
            match self.exec(s)? {
                Flow::Normal => {}
                other => return Ok(other),
            }
        }
        Ok(Flow::Normal)
    }

    fn exec(&mut self, stmt: &Stmt) -> Result<Flow, String> {
        match stmt {
            Stmt::Noop => Ok(Flow::Normal),
            Stmt::Assign { var, expr } => {
                let v = self.eval(expr)?;
                self.env.insert(var.to_ascii_lowercase(), v);
                Ok(Flow::Normal)
            }
            Stmt::Return(None) => Ok(Flow::Return(Val::Null)),
            Stmt::Return(Some(e)) => Ok(Flow::Return(self.eval(e)?)),
            Stmt::If { arms, els } => {
                for (cond, body) in arms {
                    if self.eval_bool(cond)? {
                        return self.exec_list(body);
                    }
                }
                if let Some(body) = els {
                    return self.exec_list(body);
                }
                Ok(Flow::Normal)
            }
            Stmt::While { cond, body } => {
                while self.eval_bool(cond)? {
                    self.tick()?;
                    match self.exec_list(body)? {
                        Flow::Return(v) => return Ok(Flow::Return(v)),
                        Flow::Exit => break,
                        Flow::Continue | Flow::Normal => {}
                    }
                }
                Ok(Flow::Normal)
            }
            Stmt::Loop { body } => {
                loop {
                    self.tick()?;
                    match self.exec_list(body)? {
                        Flow::Return(v) => return Ok(Flow::Return(v)),
                        Flow::Exit => break,
                        Flow::Continue | Flow::Normal => {}
                    }
                }
                Ok(Flow::Normal)
            }
            Stmt::For {
                var,
                reverse,
                lo,
                hi,
                step,
                body,
            } => {
                let lo = self.eval(lo)?.as_int()?;
                let hi = self.eval(hi)?.as_int()?;
                let step = match step {
                    Some(s) => self.eval(s)?.as_int()?.abs().max(1),
                    None => 1,
                };
                let key = var.to_ascii_lowercase();
                let mut i = lo;
                // Postgres FOR range is inclusive of both bounds.
                loop {
                    let done = if *reverse { i < hi } else { i > hi };
                    if done {
                        break;
                    }
                    self.tick()?;
                    self.env.insert(key.clone(), Val::Int(i));
                    match self.exec_list(body)? {
                        Flow::Return(v) => return Ok(Flow::Return(v)),
                        Flow::Exit => break,
                        Flow::Continue | Flow::Normal => {}
                    }
                    if *reverse {
                        i -= step;
                    } else {
                        i += step;
                    }
                }
                Ok(Flow::Normal)
            }
            Stmt::Exit { when } => match when {
                Some(c) if !self.eval_bool(c)? => Ok(Flow::Normal),
                _ => Ok(Flow::Exit),
            },
            Stmt::Continue { when } => match when {
                Some(c) if !self.eval_bool(c)? => Ok(Flow::Normal),
                _ => Ok(Flow::Continue),
            },
            Stmt::Raise { fatal, message } => {
                let msg = message.clone().unwrap_or_else(|| "raised".to_string());
                if *fatal {
                    Err(format!("plpgsql RAISE EXCEPTION: {msg}"))
                } else {
                    Ok(Flow::Normal)
                }
            }
            Stmt::SelectInto { vars, select_sql } => {
                let sql = substitute_vars(select_sql, &self.env);
                let res = self.query(&sql)?;
                let row = res.rows.first();
                for (i, var) in vars.iter().enumerate() {
                    let v = row
                        .and_then(|r| r.get(i))
                        .map(Val::from_json)
                        .unwrap_or(Val::Null);
                    self.env.insert(var.to_ascii_lowercase(), v);
                }
                Ok(Flow::Normal)
            }
            Stmt::Perform(sql) => {
                let sql = substitute_vars(sql, &self.env);
                self.query(&sql)?;
                Ok(Flow::Normal)
            }
            Stmt::Block(body) => self.exec_list(body),
        }
    }
}

/// Execute a parsed body against `args` (bound by declared parameter name), returning the
/// `RETURN`ed value (or `Null` if control falls off the end) (CONCEPT:EG-KG.query.concept-7).
fn exec_function(
    f: &StoredFunction,
    body: &PlBody,
    args: Vec<Val>,
    run_sql: &dyn Fn(&str) -> Result<TypedQueryResult, String>,
) -> Result<Val, String> {
    let mut env: HashMap<String, Val> = HashMap::new();
    for (arg, val) in f.args.iter().zip(args) {
        env.insert(arg.name.to_ascii_lowercase(), val);
    }
    let mut interp = Interp {
        env,
        run_sql,
        steps: 0,
    };
    // Initialize DECLARE variables (NULL, or their evaluated init), in order.
    for d in &body.decls {
        let v = match &d.init {
            Some(e) => interp.eval(e)?,
            None => Val::Null,
        };
        interp.env.insert(d.name.to_ascii_lowercase(), v);
    }
    match interp.exec_list(&body.body)? {
        Flow::Return(v) => Ok(v),
        _ => Ok(Val::Null),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Bare-call interception
// ─────────────────────────────────────────────────────────────────────────────

/// If `sql` is a bare top-level `SELECT fn(args) [AS alias]` or `CALL fn(args)` that names
/// a `LANGUAGE plpgsql` function in `functions`, run the interpreter and return its result
/// (CONCEPT:EG-KG.query.eg-validate-procedural-body). Returns `Ok(None)` when the statement is NOT such a call (so the
/// caller runs the normal SQL path). `run_sql` executes embedded SQL back through the same
/// read path.
pub(super) fn try_exec_call(
    sql: &str,
    functions: &[StoredFunction],
    run_sql: &dyn Fn(&str) -> Result<TypedQueryResult, String>,
) -> Result<Option<TypedQueryResult>, String> {
    let Some((is_call, name, arg_texts, alias)) = parse_bare_call(sql) else {
        return Ok(None);
    };
    let Some(f) = functions
        .iter()
        .find(|f| f.is_plpgsql() && f.name.eq_ignore_ascii_case(&name))
    else {
        return Ok(None);
    };
    if arg_texts.len() != f.args.len() {
        return Err(format!(
            "function `{}` expects {} argument(s), got {}",
            f.name,
            f.args.len(),
            arg_texts.len()
        ));
    }
    // Evaluate each call argument as a scalar in the outer (empty) environment.
    let mut args = Vec::with_capacity(arg_texts.len());
    for a in &arg_texts {
        let res = run_sql(&format!("SELECT ({a})"))?;
        let v = res
            .rows
            .first()
            .and_then(|r| r.first())
            .map(Val::from_json)
            .unwrap_or(Val::Null);
        args.push(v);
    }
    let body = parse_body(&f.body)?;
    let ret = exec_function(f, &body, args, run_sql)?;
    if is_call {
        // A `CALL proc(...)` returns no result set (like Postgres).
        return Ok(Some(TypedQueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
        }));
    }
    let col_name = alias.unwrap_or_else(|| f.name.clone());
    Ok(Some(TypedQueryResult {
        columns: vec![TypedColumn {
            name: col_name,
            ty: ret.col_type(),
        }],
        rows: vec![vec![ret.to_json()]],
    }))
}

/// Decode a bare call statement. Returns `(is_call, fn_name, arg_texts, alias)`:
/// `is_call` is true for `CALL …`, false for `SELECT …`. `None` when the statement is not
/// a simple single-function-call shape (anything with extra projection/`FROM`/operators).
fn parse_bare_call(sql: &str) -> Option<(bool, String, Vec<String>, Option<String>)> {
    let s = sql.trim().trim_end_matches(';').trim();
    let (is_call, rest) = if let Some(r) = strip_word(s, "call") {
        (true, r)
    } else {
        (false, strip_word(s, "select")?)
    };
    let rest = rest.trim_start();
    // Function name.
    let name_end = rest
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(rest.len());
    if name_end == 0 {
        return None;
    }
    let name = rest[..name_end].to_string();
    let after = rest[name_end..].trim_start();
    if !after.starts_with('(') {
        return None;
    }
    let (inner, after_paren) = read_parens(after)?;
    let arg_texts = split_top_commas(inner);
    let tail = after_paren.trim();
    // For CALL: nothing may follow. For SELECT: only an optional `[AS] alias`.
    let alias = if is_call {
        if !tail.is_empty() {
            return None;
        }
        None
    } else if tail.is_empty() {
        None
    } else {
        let after_as = strip_word(tail, "as").unwrap_or(tail).trim();
        // The alias must be a single bare identifier and nothing else.
        if after_as.is_empty()
            || !after_as
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return None;
        }
        Some(after_as.to_string())
    };
    Some((is_call, name, arg_texts, alias))
}

/// Strip a leading whole-word keyword (case-insensitive), returning the trimmed tail.
fn strip_word<'a>(s: &'a str, kw: &str) -> Option<&'a str> {
    let s = s.trim_start();
    if s.len() >= kw.len() && s[..kw.len()].eq_ignore_ascii_case(kw) {
        let after = &s[kw.len()..];
        if after
            .chars()
            .next()
            .is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_'))
        {
            return Some(after.trim_start());
        }
    }
    None
}

/// Read a balanced `(…)` whose `(` is at the start of `s`. Returns the inner text and the
/// remainder after the matching `)`. Skips `'…'` string literals.
/// Advance past a `'...'` string literal starting at `b[start] == '\''`
/// (Postgres `''` escape). Split out of `read_parens` (extract-method,
/// cx/wD8) — same terms, same order as before. Returns the index just past
/// the closing quote (or `b.len()` if unterminated).
fn skip_plpgsql_string_literal(b: &[u8], start: usize) -> usize {
    let mut i = start + 1;
    while i < b.len() {
        if b[i] == b'\'' {
            if b.get(i + 1) == Some(&b'\'') {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    i
}

fn read_parens(s: &str) -> Option<(&str, &str)> {
    let b = s.as_bytes();
    if b.first() != Some(&b'(') {
        return None;
    }
    let mut depth = 0usize;
    let mut i = 0usize;
    while i < b.len() {
        let c = b[i];
        if c == b'\'' {
            i = skip_plpgsql_string_literal(b, i);
            continue;
        }
        match c {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some((&s[1..i], &s[i + 1..]));
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Split `inner` at top-level commas (not nested in parens or string literals), trimming
/// each piece. Empty/whitespace `inner` ⇒ no arguments.
fn split_top_commas(inner: &str) -> Vec<String> {
    if inner.trim().is_empty() {
        return Vec::new();
    }
    let b = inner.as_bytes();
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    let mut i = 0usize;
    let mut in_str = false;
    while i < b.len() {
        let c = b[i];
        if in_str {
            if c == b'\'' {
                if b.get(i + 1) == Some(&b'\'') {
                    i += 2;
                    continue;
                }
                in_str = false;
            }
            i += 1;
            continue;
        }
        match c {
            b'\'' => in_str = true,
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

/// Replace whole-word identifiers matching an environment variable with the variable's
/// SQL literal (CONCEPT:EG-KG.query.concept-7). Quote-aware (a name inside `'…'` is untouched) and a
/// qualified `t.col` reference is left alone (only bare variable names are substituted).
fn substitute_vars(sql: &str, env: &HashMap<String, Val>) -> String {
    let b = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;
    let mut in_str = false;
    while i < b.len() {
        let c = b[i];
        if in_str {
            out.push(c as char);
            if c == b'\'' {
                if b.get(i + 1) == Some(&b'\'') {
                    out.push('\'');
                    i += 2;
                    continue;
                }
                in_str = false;
            }
            i += 1;
            continue;
        }
        if c == b'\'' {
            in_str = true;
            out.push('\'');
            i += 1;
            continue;
        }
        if c.is_ascii_alphabetic() || c == b'_' {
            let start = i;
            i += 1;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                i += 1;
            }
            let word = &sql[start..i];
            let qualified = start > 0 && b[start - 1] == b'.';
            if !qualified {
                if let Some(v) = env.get(&word.to_ascii_lowercase()) {
                    out.push_str(&v.to_sql_literal());
                    continue;
                }
            }
            out.push_str(word);
            continue;
        }
        out.push(c as char);
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_for_loop_and_if() {
        let body = parse_body(
            "DECLARE total int := 0; BEGIN FOR i IN 1..10 LOOP total := total + i; END LOOP; \
             IF total > 40 THEN RETURN total; ELSE RETURN 0; END IF; END",
        )
        .unwrap();
        assert_eq!(body.decls.len(), 1);
        assert_eq!(body.body.len(), 2);
    }

    #[test]
    fn parse_bare_call_shapes() {
        let (is_call, name, args, alias) = parse_bare_call("SELECT f(1, 2) AS s").unwrap();
        assert!(!is_call);
        assert_eq!(name, "f");
        assert_eq!(args, vec!["1", "2"]);
        assert_eq!(alias.as_deref(), Some("s"));

        let (is_call, name, args, _) = parse_bare_call("CALL p(3)").unwrap();
        assert!(is_call);
        assert_eq!(name, "p");
        assert_eq!(args, vec!["3"]);

        // Not a bare call — a projection with a FROM clause.
        assert!(parse_bare_call("SELECT f(x) FROM t").is_none());
        // Not a bare call — an arithmetic expression around the call.
        assert!(parse_bare_call("SELECT f(1) + 1").is_none());
    }

    #[test]
    fn substitutes_variables_quote_aware() {
        let mut env = HashMap::new();
        env.insert("i".to_string(), Val::Int(5));
        env.insert("name".to_string(), Val::Text("a'b".to_string()));
        assert_eq!(substitute_vars("i + 1", &env), "5 + 1");
        assert_eq!(substitute_vars("'i is i'", &env), "'i is i'");
        assert_eq!(substitute_vars("name", &env), "'a''b'");
        // Qualified reference untouched.
        assert_eq!(substitute_vars("t.i", &env), "t.i");
    }
}
