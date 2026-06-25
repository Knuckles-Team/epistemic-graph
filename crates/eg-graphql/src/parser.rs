//! A minimal, dependency-free GraphQL **query** parser (CONCEPT:KG-2.235).
//!
//! Covers the read subset the resolver needs — the same subset a relational/graph DB
//! GraphQL surface exposes: an anonymous (or named) `query` operation whose selection
//! set is one-or-more ROOT fields (each a node TYPE), each carrying optional ARGUMENTS
//! (`first`/`limit` ints + property-equality filters) and a nested SELECTION SET of
//! scalar fields (node properties) and object fields (edge relationships, recursed).
//!
//! NOT async-graphql: a hand-written tokenizer + recursive-descent parser, pure Rust,
//! so the surface stays Pi-excludable (the facade gates the whole crate behind
//! `graphql`). Mutations / subscriptions / fragments / variables / directives are
//! explicit deferreds — a parse error names them clearly rather than mis-parsing.

use std::fmt;

/// A parsed GraphQL value used as an argument (the read subset: ints, floats, strings,
/// booleans). Enough to express `first: 10` and `name: "Alice"`.
#[derive(Clone, Debug, PartialEq)]
pub enum GqlValue {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
}

/// One field in a selection set: a name, optional arguments, and a nested selection
/// (empty for a scalar leaf). `alias` is the response key when an `alias: name` form is
/// used (GraphQL aliasing); it defaults to `name`.
#[derive(Clone, Debug, PartialEq)]
pub struct Field {
    /// The response key — the alias if given, else `name`.
    pub alias: String,
    /// The graph field being resolved (the node label / property / edge relationship).
    pub name: String,
    pub args: Vec<(String, GqlValue)>,
    pub selection: Vec<Field>,
}

/// A parsed query operation: the top-level selection set (its fields are the root node
/// types). The operation name (if any) is irrelevant to execution.
#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    pub roots: Vec<Field>,
}

/// A GraphQL parse error with the byte offset it occurred at.
#[derive(Clone, Debug, PartialEq)]
pub struct GqlError {
    pub msg: String,
    pub at: usize,
}

impl fmt::Display for GqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GraphQL parse error at byte {}: {}", self.at, self.msg)
    }
}

impl std::error::Error for GqlError {}

/// Parse a GraphQL document into a [`Query`]. Accepts a bare selection set (`{ … }`),
/// an `query { … }`, or `query Name { … }`. Rejects mutation/subscription/fragments
/// with a clear message.
pub fn parse(src: &str) -> Result<Query, GqlError> {
    let toks = lex(src)?;
    let mut p = P {
        toks: &toks,
        pos: 0,
        end: src.len(),
    };
    let q = p.parse_document()?;
    p.expect_eof()?;
    Ok(q)
}

// ── lexer ───────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Name(String),
    Int(i64),
    Float(f64),
    Str(String),
    LBrace,
    RBrace,
    LParen,
    RParen,
    Colon,
    Comma,
    Bang,
}

#[derive(Clone, Debug, PartialEq)]
struct Token {
    kind: Tok,
    start: usize,
}

fn lex(src: &str) -> Result<Vec<Token>, GqlError> {
    let b = src.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    while i < b.len() {
        let c = b[i];
        // whitespace + commas-as-whitespace (GraphQL) + `#` line comments.
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if c == b'#' {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        let start = i;
        match c {
            b'{' => push(&mut out, Tok::LBrace, &mut i),
            b'}' => push(&mut out, Tok::RBrace, &mut i),
            b'(' => push(&mut out, Tok::LParen, &mut i),
            b')' => push(&mut out, Tok::RParen, &mut i),
            b':' => push(&mut out, Tok::Colon, &mut i),
            b',' => push(&mut out, Tok::Comma, &mut i),
            b'!' => push(&mut out, Tok::Bang, &mut i),
            b'"' => {
                let (s, next) = lex_str(b, i)?;
                out.push(Token {
                    kind: Tok::Str(s),
                    start,
                });
                i = next;
            }
            b'-' | b'0'..=b'9' => {
                let (t, next) = lex_num(src, b, i)?;
                out.push(Token { kind: t, start });
                i = next;
            }
            _ if is_name_start(c) => {
                let mut j = i + 1;
                while j < b.len() && is_name_continue(b[j]) {
                    j += 1;
                }
                out.push(Token {
                    kind: Tok::Name(src[i..j].to_string()),
                    start,
                });
                i = j;
            }
            _ => {
                return Err(GqlError {
                    msg: format!("unexpected character `{}`", c as char),
                    at: start,
                })
            }
        }
    }
    Ok(out)
}

fn push(out: &mut Vec<Token>, kind: Tok, i: &mut usize) {
    out.push(Token { kind, start: *i });
    *i += 1;
}

fn is_name_start(c: u8) -> bool {
    c.is_ascii_alphabetic() || c == b'_'
}
fn is_name_continue(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

fn lex_str(b: &[u8], start: usize) -> Result<(String, usize), GqlError> {
    let mut i = start + 1;
    let mut s = String::new();
    while i < b.len() {
        let c = b[i];
        if c == b'"' {
            return Ok((s, i + 1));
        }
        if c == b'\\' && i + 1 < b.len() {
            let e = b[i + 1];
            s.push(match e {
                b'n' => '\n',
                b't' => '\t',
                b'"' => '"',
                b'\\' => '\\',
                _ => e as char,
            });
            i += 2;
            continue;
        }
        s.push(c as char);
        i += 1;
    }
    Err(GqlError {
        msg: "unterminated string".into(),
        at: start,
    })
}

fn lex_num(src: &str, b: &[u8], start: usize) -> Result<(Tok, usize), GqlError> {
    let mut i = start;
    if b[i] == b'-' {
        i += 1;
    }
    let mut is_float = false;
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_digit() {
            i += 1;
        } else if (c == b'.' || c == b'e' || c == b'E' || c == b'+' || c == b'-') && i > start {
            is_float = is_float || c == b'.' || c == b'e' || c == b'E';
            i += 1;
        } else {
            break;
        }
    }
    let text = &src[start..i];
    if is_float {
        text.parse::<f64>()
            .map(|f| (Tok::Float(f), i))
            .map_err(|_| GqlError {
                msg: format!("invalid number `{text}`"),
                at: start,
            })
    } else {
        text.parse::<i64>()
            .map(|n| (Tok::Int(n), i))
            .map_err(|_| GqlError {
                msg: format!("invalid integer `{text}`"),
                at: start,
            })
    }
}

// ── parser ──────────────────────────────────────────────────────────────────────

struct P<'a> {
    toks: &'a [Token],
    pos: usize,
    end: usize,
}

impl P<'_> {
    fn parse_document(&mut self) -> Result<Query, GqlError> {
        // Optional `query` / `query Name` prefix. `mutation`/`subscription`/`fragment`
        // are explicit deferreds.
        if let Some(Tok::Name(kw)) = self.peek() {
            match kw.as_str() {
                "query" => {
                    self.bump();
                    // optional operation name
                    if matches!(self.peek(), Some(Tok::Name(_))) {
                        self.bump();
                    }
                }
                "mutation" => {
                    return Err(self.err("GraphQL mutations are not supported (read-only surface)"))
                }
                "subscription" => return Err(self.err("GraphQL subscriptions are not supported")),
                "fragment" => return Err(self.err("GraphQL fragments are not supported")),
                _ => {} // a bare selection set whose first field is a Name
            }
        }
        let roots = self.parse_selection_set()?;
        Ok(Query { roots })
    }

    fn parse_selection_set(&mut self) -> Result<Vec<Field>, GqlError> {
        self.expect(&Tok::LBrace, "`{` to open a selection set")?;
        let mut fields = Vec::new();
        while !self.peek_is(&Tok::RBrace) && self.peek().is_some() {
            fields.push(self.parse_field()?);
        }
        self.expect(&Tok::RBrace, "`}` to close the selection set")?;
        if fields.is_empty() {
            return Err(self.err("a selection set must select at least one field"));
        }
        Ok(fields)
    }

    fn parse_field(&mut self) -> Result<Field, GqlError> {
        let first = self.expect_name("a field name")?;
        // `alias: name` — a colon after the first name makes it the response alias.
        let (alias, name) = if self.eat(&Tok::Colon) {
            let real = self.expect_name("a field name after the alias `:`")?;
            (first, real)
        } else {
            (first.clone(), first)
        };
        let args = if self.peek_is(&Tok::LParen) {
            self.parse_args()?
        } else {
            Vec::new()
        };
        let selection = if self.peek_is(&Tok::LBrace) {
            self.parse_selection_set()?
        } else {
            Vec::new()
        };
        Ok(Field {
            alias,
            name,
            args,
            selection,
        })
    }

    fn parse_args(&mut self) -> Result<Vec<(String, GqlValue)>, GqlError> {
        self.expect(&Tok::LParen, "`(`")?;
        let mut args = Vec::new();
        while !self.peek_is(&Tok::RParen) && self.peek().is_some() {
            let name = self.expect_name("an argument name")?;
            self.expect(&Tok::Colon, "`:` after the argument name")?;
            let value = self.parse_value()?;
            args.push((name, value));
            // commas are optional (lexed as whitespace), but tolerate a stray one.
            let _ = self.eat(&Tok::Comma);
        }
        self.expect(&Tok::RParen, "`)` to close the arguments")?;
        Ok(args)
    }

    fn parse_value(&mut self) -> Result<GqlValue, GqlError> {
        match self.peek().cloned() {
            Some(Tok::Int(n)) => {
                self.bump();
                Ok(GqlValue::Int(n))
            }
            Some(Tok::Float(f)) => {
                self.bump();
                Ok(GqlValue::Float(f))
            }
            Some(Tok::Str(s)) => {
                self.bump();
                Ok(GqlValue::Str(s))
            }
            Some(Tok::Name(n)) if n == "true" || n == "false" => {
                self.bump();
                Ok(GqlValue::Bool(n == "true"))
            }
            _ => Err(self.err("expected an argument value (int, float, string, or bool)")),
        }
    }

    // ── token helpers ───────────────────────────────────────────────────────────

    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos).map(|t| &t.kind)
    }
    fn peek_is(&self, k: &Tok) -> bool {
        self.peek() == Some(k)
    }
    fn bump(&mut self) {
        self.pos += 1;
    }
    fn eat(&mut self, k: &Tok) -> bool {
        if self.peek_is(k) {
            self.bump();
            true
        } else {
            false
        }
    }
    fn expect(&mut self, k: &Tok, what: &str) -> Result<(), GqlError> {
        if self.eat(k) {
            Ok(())
        } else {
            Err(self.err(&format!("expected {what}")))
        }
    }
    fn expect_name(&mut self, what: &str) -> Result<String, GqlError> {
        match self.peek() {
            Some(Tok::Name(n)) => {
                let n = n.clone();
                self.bump();
                Ok(n)
            }
            _ => Err(self.err(&format!("expected {what}"))),
        }
    }
    fn expect_eof(&mut self) -> Result<(), GqlError> {
        if self.pos >= self.toks.len() {
            Ok(())
        } else {
            Err(self.err("unexpected trailing tokens after the query"))
        }
    }
    fn err(&self, msg: &str) -> GqlError {
        let at = self.toks.get(self.pos).map(|t| t.start).unwrap_or(self.end);
        GqlError {
            msg: msg.to_string(),
            at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nested_query_with_args() {
        let q = parse(
            r#"{
                Person(first: 2, name: "Alice") {
                    name
                    knows { name }
                }
            }"#,
        )
        .unwrap();
        assert_eq!(q.roots.len(), 1);
        let person = &q.roots[0];
        assert_eq!(person.name, "Person");
        assert_eq!(person.args.len(), 2);
        assert_eq!(person.args[0], ("first".into(), GqlValue::Int(2)));
        assert_eq!(
            person.args[1],
            ("name".into(), GqlValue::Str("Alice".into()))
        );
        assert_eq!(person.selection.len(), 2);
        assert_eq!(person.selection[0].name, "name");
        let knows = &person.selection[1];
        assert_eq!(knows.name, "knows");
        assert_eq!(knows.selection[0].name, "name");
    }

    #[test]
    fn accepts_query_keyword_and_name() {
        let q = parse("query Q { Doc { title } }").unwrap();
        assert_eq!(q.roots[0].name, "Doc");
    }

    #[test]
    fn rejects_mutation() {
        let e = parse("mutation { addDoc { id } }").unwrap_err();
        assert!(e.msg.contains("mutation"), "got {}", e.msg);
    }

    #[test]
    fn empty_selection_is_error() {
        let e = parse("{ }").unwrap_err();
        assert!(e.msg.contains("at least one field"), "got {}", e.msg);
    }
}
