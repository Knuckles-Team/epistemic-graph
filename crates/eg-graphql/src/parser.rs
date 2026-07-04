//! A minimal, dependency-free GraphQL parser (CONCEPT:EG-KG.query.sparql-completeness, writes CONCEPT:EG-KG.query.mutation).
//!
//! Covers the read subset the resolver needs — the same subset a relational/graph DB
//! GraphQL surface exposes: an anonymous (or named) `query` operation whose selection
//! set is one-or-more ROOT fields (each a node TYPE), each carrying optional ARGUMENTS
//! (`first`/`limit` ints + property-equality filters) and a nested SELECTION SET of
//! scalar fields (node properties) and object fields (edge relationships, recursed).
//!
//! It ALSO parses `mutation` and `subscription` operations (CONCEPT:EG-KG.query.mutation). A mutation
//! is a selection set of write root fields (`createNode`/`updateNode`/`deleteNode`/
//! `addEdge`/`removeEdge`) whose arguments may carry OBJECT / LIST values (the `props`
//! map). A subscription mirrors a query's selection set (the resolver serves it as a
//! poll over the current matches — see `crate::subscription`).
//!
//! NOT async-graphql: a hand-written tokenizer + recursive-descent parser, pure Rust,
//! so the surface stays Pi-excludable (the facade gates the whole crate behind
//! `graphql`).
//!
//! ## Fragments / variables / directives (CONCEPT:EG-KG.query.fragments-variables-directives)
//! The lexer also emits `$` (variable refs), `@` (directives), `...` (spreads) and `=`
//! (variable defaults). [`parse_raw`] yields a [`RawDocument`] that retains named
//! fragment definitions, fragment spreads / inline fragments, operation variable
//! definitions, and field/spread directives. The resolver
//! ([`crate::resolver::flatten_document`]) inlines the spreads, applies `@skip`/
//! `@include`, and substitutes variable references — so the [`Query`]/[`Operation`]
//! the rest of the crate sees is a plain, already-desugared selection of [`Field`]s.

use std::fmt;

use serde_json::Value;

/// A parsed GraphQL value used as an argument. The read subset is ints, floats,
/// strings, booleans (enough for `first: 10` and `name: "Alice"`); writes
/// (CONCEPT:EG-KG.query.mutation) add OBJECT and LIST values so a mutation can carry a `props`
/// map, e.g. `createNode(label: "Person", props: {name: "Alice", tags: ["a", "b"]})`.
/// CONCEPT:EG-KG.query.fragments-variables-directives adds [`GqlValue::Var`] — an unresolved `$name` reference the resolver
/// substitutes from the execution variables before the value is used.
#[derive(Clone, Debug, PartialEq)]
pub enum GqlValue {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    /// A nested input object — an ordered list of `(field, value)` pairs.
    Object(Vec<(String, GqlValue)>),
    /// A list of values.
    List(Vec<GqlValue>),
    /// The `null` literal.
    Null,
    /// A `$name` variable reference (CONCEPT:EG-KG.query.fragments-variables-directives), substituted at execution time.
    Var(String),
}

/// One field in a selection set: a name, optional arguments, and a nested selection
/// (empty for a scalar leaf). `alias` is the response key when an `alias: name` form is
/// used (GraphQL aliasing); it defaults to `name`.
///
/// This is the DESUGARED field the resolver/mutation paths consume: by the time a
/// [`Field`] exists, fragment spreads have been inlined, `@skip`/`@include` applied, and
/// `$var` argument refs substituted (CONCEPT:EG-KG.query.fragments-variables-directives).
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

/// A parsed mutation operation (CONCEPT:EG-KG.query.mutation): the top-level selection set whose
/// fields are WRITE root fields (`createNode`/`updateNode`/`deleteNode`/`addEdge`/
/// `removeEdge`). Each field's args carry the write payload; its selection set (if any)
/// shapes the returned object the resolver materializes from the post-write graph.
#[derive(Clone, Debug, PartialEq)]
pub struct Mutation {
    pub roots: Vec<Field>,
}

/// A parsed subscription operation (CONCEPT:EG-KG.query.mutation): structurally identical to a
/// [`Query`] — the resolver serves it as a poll over the current matches (and, when a
/// streaming transport is wired, as a change-stream emitting the same shape).
#[derive(Clone, Debug, PartialEq)]
pub struct Subscription {
    pub roots: Vec<Field>,
}

/// A parsed top-level GraphQL operation: a read query, a write mutation, or a
/// subscription (CONCEPT:EG-KG.query.mutation). [`parse`] returns the [`Query`] case directly for the
/// read path; [`parse_operation`] returns the full enum for callers that also write.
#[derive(Clone, Debug, PartialEq)]
pub enum Operation {
    Query(Query),
    Mutation(Mutation),
    Subscription(Subscription),
}

// ── raw (pre-desugar) AST (CONCEPT:EG-KG.query.fragments-variables-directives) ───────────────────────────────────────
//
// `parse_raw` produces this richer tree; `crate::resolver::flatten_document` lowers it
// to the plain `Field` tree above by inlining fragments, applying directives, and
// substituting variables.

/// A `@name(arg: value, …)` directive on a field, spread, or inline fragment.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Directive {
    pub name: String,
    pub args: Vec<(String, GqlValue)>,
}

/// An operation variable definition `$name: Type = default` (the `Type` is parsed but
/// ignored — this surface is untyped; only the name + optional default matter).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct VarDef {
    pub name: String,
    pub default: Option<GqlValue>,
}

/// A field in a RAW selection set — may still carry directives and contain nested raw
/// selections (which may themselves be spreads / inline fragments).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RawField {
    pub alias: String,
    pub name: String,
    pub args: Vec<(String, GqlValue)>,
    pub directives: Vec<Directive>,
    pub selections: Vec<RawSelection>,
}

/// One member of a raw selection set: a field, a fragment spread (`...Name`), or an
/// inline fragment (`... on Type { … }` / `... { … }`).
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RawSelection {
    Field(RawField),
    Spread {
        name: String,
        directives: Vec<Directive>,
    },
    Inline {
        type_cond: Option<String>,
        directives: Vec<Directive>,
        selections: Vec<RawSelection>,
    },
}

/// A named fragment definition `fragment Name on Type { … }`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Fragment {
    pub name: String,
    #[allow(dead_code)]
    pub type_cond: String,
    pub selections: Vec<RawSelection>,
}

/// A fully parsed RAW document: the single operation (kind + variable defs + selection)
/// plus any named fragment definitions it references.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RawDocument {
    pub op_kind: &'static str,
    pub var_defs: Vec<VarDef>,
    pub selections: Vec<RawSelection>,
    pub fragments: Vec<Fragment>,
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

/// Convert a parsed [`GqlValue`] to a `serde_json::Value` (CONCEPT:EG-KG.query.mutation). Shared by
/// the resolver (filter literals) and the mutation executor (write payloads), so the
/// two surfaces coerce argument values identically. An unsubstituted [`GqlValue::Var`]
/// (CONCEPT:EG-KG.query.fragments-variables-directives) coerces to `null` — the resolver substitutes vars BEFORE this runs,
/// so a `Var` reaching here means it was unbound.
pub(crate) fn gql_to_json(v: &GqlValue) -> Value {
    match v {
        GqlValue::Int(n) => Value::Number((*n).into()),
        GqlValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        GqlValue::Str(s) => Value::String(s.clone()),
        GqlValue::Bool(b) => Value::Bool(*b),
        GqlValue::Null => Value::Null,
        GqlValue::Var(_) => Value::Null,
        GqlValue::List(items) => Value::Array(items.iter().map(gql_to_json).collect()),
        GqlValue::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(k, val)| (k.clone(), gql_to_json(val)))
                .collect(),
        ),
    }
}

/// Parse a GraphQL document into a [`Query`] (the READ path). Accepts a bare selection
/// set (`{ … }`), `query { … }`, or `query Name { … }`. Fragments / variables /
/// directives (CONCEPT:EG-KG.query.fragments-variables-directives) are desugared here with NO execution variables (the
/// variable-aware entry point is [`crate::resolver::execute_with_variables`]). A
/// `mutation` / `subscription` document is reported as an error, since this entry point
/// only yields the query case.
pub fn parse(src: &str) -> Result<Query, GqlError> {
    let doc = parse_raw(src)?;
    if doc.op_kind != "query" {
        return Err(GqlError {
            msg: format!(
                "expected a query operation, found a {0} (use the {0} execution path)",
                doc.op_kind
            ),
            at: 0,
        });
    }
    let roots = crate::resolver::flatten_document(&doc, &crate::resolver::Variables::new())
        .map_err(|msg| GqlError { msg, at: 0 })?;
    Ok(Query { roots })
}

/// Parse a GraphQL document into an [`Operation`] (CONCEPT:EG-KG.query.mutation): a `query`,
/// `mutation`, or `subscription`. A bare selection set (`{ … }`) is a query. Fragments,
/// variables, and directives (CONCEPT:EG-KG.query.fragments-variables-directives) are desugared with no execution variables.
pub fn parse_operation(src: &str) -> Result<Operation, GqlError> {
    let doc = parse_raw(src)?;
    let roots = crate::resolver::flatten_document(&doc, &crate::resolver::Variables::new())
        .map_err(|msg| GqlError { msg, at: 0 })?;
    Ok(match doc.op_kind {
        "mutation" => Operation::Mutation(Mutation { roots }),
        "subscription" => Operation::Subscription(Subscription { roots }),
        _ => Operation::Query(Query { roots }),
    })
}

/// Parse a GraphQL document into the RAW (pre-desugar) [`RawDocument`] (CONCEPT:EG-KG.query.fragments-variables-directives),
/// retaining fragments, variable definitions, directives, and `$var` references. The
/// resolver lowers it to a plain [`Field`] tree once execution variables are known.
pub(crate) fn parse_raw(src: &str) -> Result<RawDocument, GqlError> {
    let toks = lex(src)?;
    let mut p = P {
        toks: &toks,
        pos: 0,
        end: src.len(),
    };
    let doc = p.parse_raw_document()?;
    p.expect_eof()?;
    Ok(doc)
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
    LBracket,
    RBracket,
    Colon,
    Comma,
    Bang,
    /// `$` — starts a variable reference / definition (CONCEPT:EG-KG.query.fragments-variables-directives).
    Dollar,
    /// `@` — starts a directive (CONCEPT:EG-KG.query.fragments-variables-directives).
    At,
    /// `...` — a fragment spread / inline fragment (CONCEPT:EG-KG.query.fragments-variables-directives).
    Spread,
    /// `=` — a variable-definition default separator (CONCEPT:EG-KG.query.fragments-variables-directives).
    Eq,
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
            b'[' => push(&mut out, Tok::LBracket, &mut i),
            b']' => push(&mut out, Tok::RBracket, &mut i),
            b':' => push(&mut out, Tok::Colon, &mut i),
            b',' => push(&mut out, Tok::Comma, &mut i),
            b'!' => push(&mut out, Tok::Bang, &mut i),
            b'$' => push(&mut out, Tok::Dollar, &mut i),
            b'@' => push(&mut out, Tok::At, &mut i),
            b'=' => push(&mut out, Tok::Eq, &mut i),
            b'.' => {
                // The only `.`-led token is the three-dot spread `...`.
                if i + 2 < b.len() && b[i + 1] == b'.' && b[i + 2] == b'.' {
                    out.push(Token {
                        kind: Tok::Spread,
                        start,
                    });
                    i += 3;
                } else {
                    return Err(GqlError {
                        msg: "expected `...` (a spread)".into(),
                        at: start,
                    });
                }
            }
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
    /// Parse the whole document: one operation (`query`/`mutation`/`subscription` or a
    /// bare `{ … }`) plus any number of `fragment` definitions, in any order.
    fn parse_raw_document(&mut self) -> Result<RawDocument, GqlError> {
        let mut op: Option<(&'static str, Vec<VarDef>, Vec<RawSelection>)> = None;
        let mut fragments = Vec::new();
        while self.peek().is_some() {
            match self.peek() {
                Some(Tok::Name(kw)) if kw == "fragment" => {
                    fragments.push(self.parse_fragment()?);
                }
                Some(Tok::Name(kw))
                    if kw == "query" || kw == "mutation" || kw == "subscription" =>
                {
                    if op.is_some() {
                        return Err(self.err("only a single operation is supported per document"));
                    }
                    op = Some(self.parse_operation_def()?);
                }
                Some(Tok::LBrace) => {
                    if op.is_some() {
                        return Err(self.err("only a single operation is supported per document"));
                    }
                    let selections = self.parse_raw_selection_set()?;
                    op = Some(("query", Vec::new(), selections));
                }
                _ => {
                    return Err(self.err(
                        "expected an operation (`query`/`mutation`/`subscription` or `{`) \
                         or a `fragment` definition",
                    ))
                }
            }
        }
        let (op_kind, var_defs, selections) =
            op.ok_or_else(|| self.err("a GraphQL document must contain an operation"))?;
        Ok(RawDocument {
            op_kind,
            var_defs,
            selections,
            fragments,
        })
    }

    /// `query|mutation|subscription [Name] [($v: T = d, …)] [@dir …] { … }`.
    fn parse_operation_def(
        &mut self,
    ) -> Result<(&'static str, Vec<VarDef>, Vec<RawSelection>), GqlError> {
        let kw = self.expect_name("an operation keyword")?;
        let op_kind = match kw.as_str() {
            "mutation" => "mutation",
            "subscription" => "subscription",
            _ => "query",
        };
        // optional operation name
        if matches!(self.peek(), Some(Tok::Name(_))) {
            self.bump();
        }
        let var_defs = if self.peek_is(&Tok::LParen) {
            self.parse_var_defs()?
        } else {
            Vec::new()
        };
        // operation-level directives are parsed and ignored.
        let _ = self.parse_directives()?;
        let selections = self.parse_raw_selection_set()?;
        Ok((op_kind, var_defs, selections))
    }

    /// `fragment Name on Type [@dir …] { … }`.
    fn parse_fragment(&mut self) -> Result<Fragment, GqlError> {
        // consume `fragment`
        let _ = self.expect_name("`fragment`")?;
        let name = self.expect_name("a fragment name")?;
        let on = self.expect_name("`on` after the fragment name")?;
        if on != "on" {
            return Err(self.err("expected `on` after the fragment name"));
        }
        let type_cond = self.expect_name("a type condition")?;
        let _ = self.parse_directives()?;
        let selections = self.parse_raw_selection_set()?;
        Ok(Fragment {
            name,
            type_cond,
            selections,
        })
    }

    /// `($name: Type [= default], …)` — variable definitions (CONCEPT:EG-KG.query.fragments-variables-directives).
    fn parse_var_defs(&mut self) -> Result<Vec<VarDef>, GqlError> {
        self.expect(&Tok::LParen, "`(` to open variable definitions")?;
        let mut defs = Vec::new();
        while !self.peek_is(&Tok::RParen) && self.peek().is_some() {
            self.expect(&Tok::Dollar, "`$` to start a variable definition")?;
            let name = self.expect_name("a variable name")?;
            self.expect(&Tok::Colon, "`:` after the variable name")?;
            self.parse_type_ref()?; // type consumed + ignored (untyped surface)
            let default = if self.eat(&Tok::Eq) {
                Some(self.parse_value()?)
            } else {
                None
            };
            defs.push(VarDef { name, default });
            let _ = self.eat(&Tok::Comma);
        }
        self.expect(&Tok::RParen, "`)` to close variable definitions")?;
        Ok(defs)
    }

    /// A type reference `Name`, `[Type]`, or either with a trailing `!`. Parsed for
    /// well-formedness then discarded — the surface is untyped.
    fn parse_type_ref(&mut self) -> Result<(), GqlError> {
        if self.peek_is(&Tok::LBracket) {
            self.bump();
            self.parse_type_ref()?;
            self.expect(&Tok::RBracket, "`]` to close a list type")?;
        } else {
            let _ = self.expect_name("a type name")?;
        }
        let _ = self.eat(&Tok::Bang);
        Ok(())
    }

    /// Zero or more `@name[(args)]` directives (CONCEPT:EG-KG.query.fragments-variables-directives).
    fn parse_directives(&mut self) -> Result<Vec<Directive>, GqlError> {
        let mut ds = Vec::new();
        while self.peek_is(&Tok::At) {
            self.bump();
            let name = self.expect_name("a directive name after `@`")?;
            let args = if self.peek_is(&Tok::LParen) {
                self.parse_args()?
            } else {
                Vec::new()
            };
            ds.push(Directive { name, args });
        }
        Ok(ds)
    }

    fn parse_raw_selection_set(&mut self) -> Result<Vec<RawSelection>, GqlError> {
        self.expect(&Tok::LBrace, "`{` to open a selection set")?;
        let mut sels = Vec::new();
        while !self.peek_is(&Tok::RBrace) && self.peek().is_some() {
            sels.push(self.parse_raw_selection()?);
        }
        self.expect(&Tok::RBrace, "`}` to close the selection set")?;
        if sels.is_empty() {
            return Err(self.err("a selection set must select at least one field"));
        }
        Ok(sels)
    }

    /// A field, a fragment spread (`...Name`), or an inline fragment
    /// (`... on Type { … }` / `... { … }`) — CONCEPT:EG-KG.query.fragments-variables-directives.
    fn parse_raw_selection(&mut self) -> Result<RawSelection, GqlError> {
        if self.peek_is(&Tok::Spread) {
            self.bump();
            if let Some(Tok::Name(n)) = self.peek() {
                if n == "on" {
                    // inline fragment with a type condition
                    self.bump();
                    let type_cond = Some(self.expect_name("a type condition after `on`")?);
                    let directives = self.parse_directives()?;
                    let selections = self.parse_raw_selection_set()?;
                    return Ok(RawSelection::Inline {
                        type_cond,
                        directives,
                        selections,
                    });
                }
                // a named fragment spread `...Name`
                let name = self.expect_name("a fragment name")?;
                let directives = self.parse_directives()?;
                return Ok(RawSelection::Spread { name, directives });
            }
            // inline fragment with no type condition: `... @dir { … }` / `... { … }`
            let directives = self.parse_directives()?;
            let selections = self.parse_raw_selection_set()?;
            return Ok(RawSelection::Inline {
                type_cond: None,
                directives,
                selections,
            });
        }
        Ok(RawSelection::Field(self.parse_raw_field()?))
    }

    fn parse_raw_field(&mut self) -> Result<RawField, GqlError> {
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
        let directives = self.parse_directives()?;
        let selections = if self.peek_is(&Tok::LBrace) {
            self.parse_raw_selection_set()?
        } else {
            Vec::new()
        };
        Ok(RawField {
            alias,
            name,
            args,
            directives,
            selections,
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
            Some(Tok::Name(n)) if n == "null" => {
                self.bump();
                Ok(GqlValue::Null)
            }
            // A `$name` variable reference (CONCEPT:EG-KG.query.fragments-variables-directives).
            Some(Tok::Dollar) => {
                self.bump();
                let name = self.expect_name("a variable name after `$`")?;
                Ok(GqlValue::Var(name))
            }
            // An input object `{ field: value, … }` (CONCEPT:EG-KG.query.mutation — a mutation `props`).
            Some(Tok::LBrace) => self.parse_object_value(),
            // A list `[ value, … ]`.
            Some(Tok::LBracket) => self.parse_list_value(),
            _ => Err(self.err(
                "expected an argument value (int, float, string, bool, null, \
                 variable, object, or list)",
            )),
        }
    }

    /// Parse an input-object value `{ field: value, … }` (commas optional).
    fn parse_object_value(&mut self) -> Result<GqlValue, GqlError> {
        self.expect(&Tok::LBrace, "`{` to open an input object")?;
        let mut fields = Vec::new();
        while !self.peek_is(&Tok::RBrace) && self.peek().is_some() {
            let name = self.expect_name("an input-object field name")?;
            self.expect(&Tok::Colon, "`:` after the input-object field name")?;
            let value = self.parse_value()?;
            fields.push((name, value));
            let _ = self.eat(&Tok::Comma);
        }
        self.expect(&Tok::RBrace, "`}` to close the input object")?;
        Ok(GqlValue::Object(fields))
    }

    /// Parse a list value `[ value, … ]` (commas optional).
    fn parse_list_value(&mut self) -> Result<GqlValue, GqlError> {
        self.expect(&Tok::LBracket, "`[` to open a list")?;
        let mut items = Vec::new();
        while !self.peek_is(&Tok::RBracket) && self.peek().is_some() {
            items.push(self.parse_value()?);
            let _ = self.eat(&Tok::Comma);
        }
        self.expect(&Tok::RBracket, "`]` to close the list")?;
        Ok(GqlValue::List(items))
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
    fn read_parse_rejects_mutation() {
        // The READ entry point yields only the query case (the write path uses
        // `parse_operation`), so a mutation document is reported as not-a-query.
        let e = parse("mutation { createNode(label: \"Doc\") { id } }").unwrap_err();
        assert!(e.msg.contains("mutation"), "got {}", e.msg);
    }

    #[test]
    fn parses_mutation_with_object_and_list_args() {
        let op = parse_operation(
            r#"mutation {
                createNode(label: "Person", props: {name: "Alice", tags: ["a", "b"], age: 30}) {
                    id
                    name
                }
            }"#,
        )
        .unwrap();
        let Operation::Mutation(m) = op else {
            panic!("expected a mutation");
        };
        assert_eq!(m.roots.len(), 1);
        let create = &m.roots[0];
        assert_eq!(create.name, "createNode");
        assert_eq!(
            create.args[0],
            ("label".into(), GqlValue::Str("Person".into()))
        );
        let GqlValue::Object(props) = &create.args[1].1 else {
            panic!("props must be an object");
        };
        assert_eq!(props[0], ("name".into(), GqlValue::Str("Alice".into())));
        assert_eq!(
            props[1],
            (
                "tags".into(),
                GqlValue::List(vec![GqlValue::Str("a".into()), GqlValue::Str("b".into()),])
            )
        );
        assert_eq!(props[2], ("age".into(), GqlValue::Int(30)));
        // the selection set shapes the returned object.
        assert_eq!(create.selection[0].name, "id");
        assert_eq!(create.selection[1].name, "name");
    }

    #[test]
    fn parses_subscription() {
        let op = parse_operation("subscription { Person { name } }").unwrap();
        let Operation::Subscription(s) = op else {
            panic!("expected a subscription");
        };
        assert_eq!(s.roots[0].name, "Person");
    }

    #[test]
    fn empty_selection_is_error() {
        let e = parse("{ }").unwrap_err();
        assert!(e.msg.contains("at least one field"), "got {}", e.msg);
    }

    // ── CONCEPT:EG-KG.query.fragments-variables-directives — fragments / variables / directives ──────────────────────

    #[test]
    fn raw_doc_retains_fragments_and_var_defs() {
        let doc = parse_raw(
            r#"query Q($x: Int, $active: Boolean = true) {
                Person { ...frag ... on Person { extra } }
            }
            fragment frag on Person { name @skip(if: $x) }"#,
        )
        .unwrap();
        assert_eq!(doc.op_kind, "query");
        assert_eq!(doc.var_defs.len(), 2);
        assert_eq!(doc.var_defs[0].name, "x");
        assert_eq!(doc.var_defs[1].name, "active");
        assert_eq!(doc.var_defs[1].default, Some(GqlValue::Bool(true)));
        assert_eq!(doc.fragments.len(), 1);
        assert_eq!(doc.fragments[0].name, "frag");
        // the root Person selection holds a spread + an inline fragment.
        let RawSelection::Field(person) = &doc.selections[0] else {
            panic!("expected a field");
        };
        assert!(matches!(person.selections[0], RawSelection::Spread { .. }));
        assert!(matches!(person.selections[1], RawSelection::Inline { .. }));
    }

    #[test]
    fn parses_variable_reference_in_args() {
        let doc = parse_raw("query Q($n: Int) { Person(first: $n) { name } }").unwrap();
        let RawSelection::Field(person) = &doc.selections[0] else {
            panic!("expected a field");
        };
        assert_eq!(person.args[0], ("first".into(), GqlValue::Var("n".into())));
    }
}
