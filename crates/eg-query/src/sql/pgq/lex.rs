//! Bounded SQL/PGQ lexical primitives and statement-shape recognition.

use crate::tables::property_graph::{SqlIdentifier, SqlName};

pub const MAX_PGQ_SQL_BYTES: usize = 256 * 1024;
const MAX_PGQ_TOKENS: usize = 16_384;
pub(super) const MAX_GRAPH_PATTERN_EDGES: usize = 32;
pub(super) const MAX_GRAPH_TABLE_COLUMNS: usize = 256;
pub(super) const MAX_GRAPH_TABLE_BRANCHES: usize = 64;
pub(super) const MAX_GRAPH_EXPR_DEPTH: usize = 32;
pub(super) const MAX_LABEL_EXPR_DEPTH: usize = 32;
/// Total nodes in ONE label expression. `MAX_LABEL_EXPR_DEPTH` bounds only the
/// nesting of `!` and parentheses; a flat `a|a|a|…` chain nests no deeper but
/// still builds a left spine whose length would otherwise be bounded only by
/// `MAX_PGQ_SQL_BYTES`. Every consumer of a `LabelExpr` walks that spine —
/// including the DERIVED `Drop`, `Clone` and `PartialEq` on its boxes, which no
/// amount of iterative traversal in this crate can make non-recursive — so the
/// node count is the one bound that makes all of them safe.
pub(super) const MAX_LABEL_EXPR_NODES: usize = 256;

/// Whether `sql` starts with a property-graph DDL keyword sequence owned by this
/// module. This deliberately recognizes only the current SQL/PGQ surface; once
/// recognized, callers must invoke the bounded DDL parser and propagate any
/// syntax error rather than falling through to another SQL grammar.
pub(crate) fn is_property_graph_ddl(sql: &str) -> bool {
    starts_with_keywords(sql, &["CREATE", "PROPERTY", "GRAPH"])
        || starts_with_keywords(sql, &["CREATE", "TEMP", "PROPERTY", "GRAPH"])
        || starts_with_keywords(sql, &["CREATE", "TEMPORARY", "PROPERTY", "GRAPH"])
        || starts_with_keywords(sql, &["ALTER", "PROPERTY", "GRAPH"])
        || starts_with_keywords(sql, &["DROP", "PROPERTY", "GRAPH"])
}

/// Whether `sql` is one of the bounded graph-table read shapes accepted by
/// the bounded graph-table statement parser.
pub(crate) fn is_graph_table_sql(sql: &str) -> bool {
    starts_with_keywords(sql, &["GRAPH_TABLE"]) || starts_with_select_star_from_graph_table(sql)
}

fn starts_with_keywords(mut sql: &str, keywords: &[&str]) -> bool {
    for keyword in keywords {
        sql = sql.trim_start();
        let Some(rest) = sql.get(keyword.len()..) else {
            return false;
        };
        if !sql[..keyword.len()].eq_ignore_ascii_case(keyword)
            || rest
                .chars()
                .next()
                .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        {
            return false;
        }
        sql = rest;
    }
    true
}

fn starts_with_select_star_from_graph_table(sql: &str) -> bool {
    let sql = sql.trim_start();
    let Some(after_select) = strip_ascii_keyword(sql, "SELECT") else {
        return false;
    };
    let after_select = after_select.trim_start();
    let Some(after_star) = after_select.strip_prefix('*') else {
        return false;
    };
    let Some(after_from) = strip_ascii_keyword(after_star.trim_start(), "FROM") else {
        return false;
    };
    strip_ascii_keyword(after_from.trim_start(), "GRAPH_TABLE").is_some()
}

fn strip_ascii_keyword<'a>(sql: &'a str, keyword: &str) -> Option<&'a str> {
    let rest = sql.get(keyword.len()..)?;
    if sql[..keyword.len()].eq_ignore_ascii_case(keyword)
        && !rest
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        Some(rest)
    } else {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Token {
    Word(String),
    QuotedIdentifier(String),
    StringLiteral(String),
    Number(String),
    Symbol(char),
    NotEqual,
    LessEqual,
    GreaterEqual,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Cursor {
    pub(super) tokens: Vec<Token>,
    pub(super) at: usize,
}

impl Cursor {
    pub(super) fn new(sql: &str) -> Result<Self, String> {
        Ok(Self {
            tokens: lex(sql)?,
            at: 0,
        })
    }

    pub(super) fn finish(&mut self) -> Result<(), String> {
        self.symbol(';');
        if self.at == self.tokens.len() {
            Ok(())
        } else {
            Err(format!("unexpected token {:?}", self.tokens[self.at]))
        }
    }

    pub(super) fn identifier(&mut self) -> Result<SqlIdentifier, String> {
        match self.tokens.get(self.at).cloned() {
            Some(Token::Word(value)) => {
                self.at += 1;
                SqlIdentifier::unquoted(value)
            }
            Some(Token::QuotedIdentifier(value)) => {
                self.at += 1;
                SqlIdentifier::quoted(value)
            }
            value => Err(format!("expected identifier, found {value:?}")),
        }
    }

    pub(super) fn expect_keyword(&mut self, value: &str) -> Result<(), String> {
        self.keyword(value)
            .then_some(())
            .ok_or_else(|| format!("expected keyword {value}"))
    }

    pub(super) fn keyword(&mut self, value: &str) -> bool {
        let matched = matches!(self.tokens.get(self.at), Some(Token::Word(word)) if word.eq_ignore_ascii_case(value));
        self.at += usize::from(matched);
        matched
    }

    pub(super) fn any_keyword(&self, values: &[&str]) -> bool {
        matches!(self.tokens.get(self.at), Some(Token::Word(word)) if values.iter().any(|value| word.eq_ignore_ascii_case(value)))
    }

    pub(super) fn expect(&mut self, value: char) -> Result<(), String> {
        self.symbol(value)
            .then_some(())
            .ok_or_else(|| format!("expected symbol '{value}'"))
    }

    pub(super) fn symbol(&mut self, value: char) -> bool {
        let matched = self.peek(value);
        self.at += usize::from(matched);
        matched
    }

    pub(super) fn peek(&self, value: char) -> bool {
        matches!(self.tokens.get(self.at), Some(Token::Symbol(found)) if *found == value)
    }

    pub(super) fn is_identifier(&self) -> bool {
        matches!(
            self.tokens.get(self.at),
            Some(Token::Word(_) | Token::QuotedIdentifier(_))
        )
    }
}

pub(super) fn parse_name(cursor: &mut Cursor) -> Result<SqlName, String> {
    let mut parts = vec![cursor.identifier()?];
    while cursor.symbol('.') {
        parts.push(cursor.identifier()?);
    }
    SqlName::new(parts)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlNumber(String);

impl SqlNumber {
    pub fn parse(value: &str) -> Result<Self, String> {
        let parsed = value
            .parse::<f64>()
            .map_err(|_| format!("invalid numeric literal: {value}"))?;
        if !parsed.is_finite() {
            return Err("numeric literal must be finite".into());
        }
        Ok(Self(value.into()))
    }

    pub(super) fn sql(&self) -> &str {
        &self.0
    }
}

pub(super) fn lex(sql: &str) -> Result<Vec<Token>, String> {
    if sql.is_empty() || sql.len() > MAX_PGQ_SQL_BYTES || sql.contains('\0') {
        return Err("SQL/PGQ input violates byte bounds".into());
    }
    let bytes = sql.as_bytes();
    let (mut out, mut i) = (Vec::new(), 0);
    while i < bytes.len() {
        if out.len() == MAX_PGQ_TOKENS {
            return Err("SQL/PGQ token limit exceeded".into());
        }
        let b = bytes[i];
        if b.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        let (token, next) = lex_token(sql, bytes, i)?;
        out.push(token);
        i = next;
    }
    Ok(out)
}

fn lex_token(sql: &str, bytes: &[u8], at: usize) -> Result<(Token, usize), String> {
    let byte = bytes[at];
    if is_comment_start(bytes, at) {
        return Err("comments are not accepted by the bounded SQL/PGQ parser".into());
    }
    if is_quote(byte) {
        return lex_quoted(bytes, at, byte);
    }
    if is_word_start(byte) {
        return Ok(lex_word(sql, bytes, at));
    }
    if is_number_start(bytes, at) {
        return lex_number(sql, bytes, at);
    }
    lex_symbol(bytes, at)
}

fn is_comment_start(bytes: &[u8], at: usize) -> bool {
    matches!(
        (bytes[at], bytes.get(at + 1)),
        (b'-', Some(b'-')) | (b'/', Some(b'*'))
    )
}

fn is_quote(byte: u8) -> bool {
    matches!(byte, b'\'' | b'"')
}

fn is_word_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_number_start(bytes: &[u8], at: usize) -> bool {
    bytes[at].is_ascii_digit()
        || (bytes[at] == b'-' && bytes.get(at + 1).is_some_and(|next| next.is_ascii_digit()))
}

fn lex_quoted(bytes: &[u8], at: usize, delimiter: u8) -> Result<(Token, usize), String> {
    let (value, next) = read_quoted(bytes, at, delimiter)?;
    let token = if delimiter == b'\'' {
        Token::StringLiteral(value)
    } else {
        Token::QuotedIdentifier(value)
    };
    Ok((token, next))
}

fn lex_word(sql: &str, bytes: &[u8], at: usize) -> (Token, usize) {
    let mut end = at + 1;
    while end < bytes.len()
        && (bytes[end].is_ascii_alphanumeric() || matches!(bytes[end], b'_' | b'$'))
    {
        end += 1;
    }
    (Token::Word(sql[at..end].into()), end)
}

fn lex_number(sql: &str, bytes: &[u8], at: usize) -> Result<(Token, usize), String> {
    let mut end = at + 1;
    while end < bytes.len()
        && (bytes[end].is_ascii_digit() || matches!(bytes[end], b'.' | b'e' | b'E' | b'+' | b'-'))
    {
        end += 1;
    }
    let value = &sql[at..end];
    value
        .parse::<f64>()
        .map_err(|_| format!("invalid number '{value}'"))?;
    Ok((Token::Number(value.into()), end))
}

fn lex_symbol(bytes: &[u8], at: usize) -> Result<(Token, usize), String> {
    let byte = bytes[at];
    match (byte, bytes.get(at + 1).copied()) {
        (b'<', Some(b'>')) | (b'!', Some(b'=')) => Ok((Token::NotEqual, at + 2)),
        (b'<', Some(b'=')) => Ok((Token::LessEqual, at + 2)),
        (b'>', Some(b'=')) => Ok((Token::GreaterEqual, at + 2)),
        _ if is_symbol(byte) => Ok((Token::Symbol(byte as char), at + 1)),
        _ => Err(format!("unsupported SQL/PGQ byte 0x{byte:02x}")),
    }
}

fn is_symbol(byte: u8) -> bool {
    matches!(
        byte,
        b'(' | b')'
            | b'['
            | b']'
            | b','
            | b'.'
            | b';'
            | b'|'
            | b'&'
            | b'!'
            | b':'
            | b'='
            | b'<'
            | b'>'
            | b'-'
            | b'*'
    )
}

fn read_quoted(bytes: &[u8], start: usize, delimiter: u8) -> Result<(String, usize), String> {
    let (mut out, mut i) = (Vec::new(), start + 1);
    while i < bytes.len() {
        if bytes[i] == delimiter {
            if bytes.get(i + 1) == Some(&delimiter) {
                out.push(delimiter);
                i += 2;
                continue;
            }
            return String::from_utf8(out)
                .map(|value| (value, i + 1))
                .map_err(|_| "quoted value is not UTF-8".into());
        }
        out.push(bytes[i]);
        i += 1;
    }
    Err("unterminated quoted value".into())
}
