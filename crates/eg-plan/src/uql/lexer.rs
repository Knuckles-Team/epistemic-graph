//! UQL lexer (CONCEPT:AU-KG.query.top-nodes-by-degree) — a hand-written, dependency-free tokenizer.
//!
//! Turns a UQL source string into a flat `Vec<Token>` with byte spans, so the
//! recursive-descent [`super::parser`] never touches raw characters. Pure Rust, no
//! regex / no DataFusion — the whole UQL front-end stays in the dep-free half of
//! eg-plan (the Pi contract): only EXECUTION is `query`-gated, parsing is not.

use std::fmt;

/// A lexical token plus the byte span it covers in the source (for error carets).
#[derive(Clone, Debug, PartialEq)]
pub struct Token {
    pub kind: Tok,
    /// Inclusive start byte offset into the source.
    pub start: usize,
    /// Exclusive end byte offset into the source.
    pub end: usize,
}

/// The UQL token kinds. Keywords are case-insensitive and recognized as such only
/// where the grammar expects them; everywhere else a bare word is an [`Tok::Ident`].
#[derive(Clone, Debug, PartialEq)]
pub enum Tok {
    /// A bare identifier / keyword (label, property name, relationship, MATCH, …).
    /// Keyword classification is done by the parser (case-insensitive compare), so the
    /// lexer stays context-free.
    Ident(String),
    /// A numeric literal (always parsed as f64; integers are a subset).
    Num(f64),
    /// A single- or double-quoted string literal (quotes stripped, `''`/`\"` unescaped).
    Str(String),
    /// An angle-bracketed IRI `<scheme:...>` (CONCEPT:EG-KG.query.reason-iri-parses-angle) — a whitespace-free
    /// `<...>` run whose interior carries a `:` (a scheme). Stored WITH its brackets
    /// (`<http://ex/Device>`) so it round-trips as a canonical OWL class id for
    /// `REASON <iri>`. Distinguished from the comparison `<` (which is followed by a
    /// numeric RHS, e.g. `year < 2022`), so both coexist.
    Iri(String),
    /// `|>` — the pipeline stage separator.
    Pipe,
    /// `(`
    LParen,
    /// `)`
    RParen,
    /// `{`
    LBrace,
    /// `}`
    RBrace,
    /// `:`
    Colon,
    /// `,`
    Comma,
    /// `>`
    Gt,
    /// `<`
    Lt,
    /// `=` / `==` (both mean equality in UQL).
    Eq,
    /// `..` — the hop-range separator inside `{min,max}` / `{min..max}`.
    DotDot,
    /// `->` — the (forward) traversal arrow.
    Arrow,
    /// `-` — a lone dash (edge-pattern dash, e.g. `-[:CITES]->`).
    Dash,
    /// `[`
    LBracket,
    /// `]`
    RBracket,
    /// `~` — the vector-rank sigil (`RANK BY ~query`).
    Tilde,
    /// `@` — RESERVED for the future time/asof seam (`AS OF @t`); lexed, not parsed.
    At,
}

impl fmt::Display for Tok {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Tok::Ident(s) => write!(f, "`{s}`"),
            Tok::Num(n) => write!(f, "number `{n}`"),
            Tok::Str(s) => write!(f, "string \"{s}\""),
            Tok::Iri(s) => write!(f, "IRI `{s}`"),
            Tok::Pipe => f.write_str("`|>`"),
            Tok::LParen => f.write_str("`(`"),
            Tok::RParen => f.write_str("`)`"),
            Tok::LBrace => f.write_str("`{`"),
            Tok::RBrace => f.write_str("`}`"),
            Tok::Colon => f.write_str("`:`"),
            Tok::Comma => f.write_str("`,`"),
            Tok::Gt => f.write_str("`>`"),
            Tok::Lt => f.write_str("`<`"),
            Tok::Eq => f.write_str("`=`"),
            Tok::DotDot => f.write_str("`..`"),
            Tok::Arrow => f.write_str("`->`"),
            Tok::Dash => f.write_str("`-`"),
            Tok::LBracket => f.write_str("`[`"),
            Tok::RBracket => f.write_str("`]`"),
            Tok::Tilde => f.write_str("`~`"),
            Tok::At => f.write_str("`@`"),
        }
    }
}

/// A lexing error with the byte offset it occurred at (rendered with a caret by the
/// parser's error formatter).
#[derive(Clone, Debug, PartialEq)]
pub struct LexError {
    pub msg: String,
    pub at: usize,
}

/// Tokenize a UQL source string. Whitespace is skipped; everything else becomes a
/// [`Token`]. Returns the first [`LexError`] on an unterminated string or a stray
/// character.
pub fn lex(src: &str) -> Result<Vec<Token>, LexError> {
    let bytes = src.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    while i < bytes.len() {
        let c = bytes[i];
        // Whitespace.
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        let start = i;
        match c {
            b'|' if peek(bytes, i + 1) == Some(b'>') => {
                out.push(tok(Tok::Pipe, start, i + 2));
                i += 2;
            }
            b'(' => simple(&mut out, &mut i, Tok::LParen),
            b')' => simple(&mut out, &mut i, Tok::RParen),
            b'{' => simple(&mut out, &mut i, Tok::LBrace),
            b'}' => simple(&mut out, &mut i, Tok::RBrace),
            b'[' => simple(&mut out, &mut i, Tok::LBracket),
            b']' => simple(&mut out, &mut i, Tok::RBracket),
            b':' => simple(&mut out, &mut i, Tok::Colon),
            b',' => simple(&mut out, &mut i, Tok::Comma),
            b'>' => simple(&mut out, &mut i, Tok::Gt),
            // `<` is either an angle-bracketed IRI (`<http://ex/Device>` — CONCEPT:EG-KG.query.reason-iri-parses-angle)
            // or the comparison `<` (`year < 2022`). Try the IRI form first; it only
            // matches a whitespace-free `<...>` run whose interior carries a `:` (a scheme),
            // which a numeric comparison never does, so the two never collide.
            b'<' => match lex_iri(bytes, i) {
                Some((iri, next)) => {
                    out.push(tok(Tok::Iri(iri), start, next));
                    i = next;
                }
                None => simple(&mut out, &mut i, Tok::Lt),
            },
            b'~' => simple(&mut out, &mut i, Tok::Tilde),
            b'@' => simple(&mut out, &mut i, Tok::At),
            b'=' => {
                // `=` or `==` both mean equality.
                let len = if peek(bytes, i + 1) == Some(b'=') {
                    2
                } else {
                    1
                };
                out.push(tok(Tok::Eq, start, i + len));
                i += len;
            }
            b'-' if peek(bytes, i + 1) == Some(b'>') => {
                out.push(tok(Tok::Arrow, start, i + 2));
                i += 2;
            }
            b'-' => simple(&mut out, &mut i, Tok::Dash),
            b'.' if peek(bytes, i + 1) == Some(b'.') => {
                out.push(tok(Tok::DotDot, start, i + 2));
                i += 2;
            }
            b'\'' | b'"' => {
                let (s, next) = lex_string(bytes, i, c)?;
                out.push(tok(Tok::Str(s), start, next));
                i = next;
            }
            b'0'..=b'9' => {
                let (n, next) = lex_number(src, bytes, i)?;
                out.push(tok(Tok::Num(n), start, next));
                i = next;
            }
            // Leading `-` followed by a digit is a negative number (e.g. a vector
            // component); a lone `-` was already handled above. A `.` starting a
            // number (e.g. `.5`) is also accepted.
            b'.' if peek(bytes, i + 1).is_some_and(|d| d.is_ascii_digit()) => {
                let (n, next) = lex_number(src, bytes, i)?;
                out.push(tok(Tok::Num(n), start, next));
                i = next;
            }
            _ if is_ident_start(c) => {
                let next = scan_while(bytes, i, is_ident_continue);
                let word = src[i..next].to_string();
                out.push(tok(Tok::Ident(word), start, next));
                i = next;
            }
            _ => {
                return Err(LexError {
                    msg: format!("unexpected character `{}`", c as char),
                    at: start,
                });
            }
        }
    }
    Ok(out)
}

fn tok(kind: Tok, start: usize, end: usize) -> Token {
    Token { kind, start, end }
}

fn simple(out: &mut Vec<Token>, i: &mut usize, kind: Tok) {
    out.push(tok(kind, *i, *i + 1));
    *i += 1;
}

fn peek(bytes: &[u8], i: usize) -> Option<u8> {
    bytes.get(i).copied()
}

fn is_ident_start(c: u8) -> bool {
    c.is_ascii_alphabetic() || c == b'_'
}

fn is_ident_continue(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

fn scan_while(bytes: &[u8], mut i: usize, pred: fn(u8) -> bool) -> usize {
    while i < bytes.len() && pred(bytes[i]) {
        i += 1;
    }
    i
}

/// Lex a quoted string starting at `start` (the opening quote `q`). Supports `''`
/// (SQL-style) inside single-quoted and `\"` / `\\` inside double-quoted strings.
fn lex_string(bytes: &[u8], start: usize, q: u8) -> Result<(String, usize), LexError> {
    let mut i = start + 1;
    let mut s = String::new();
    while i < bytes.len() {
        let c = bytes[i];
        if c == q {
            // A doubled quote is an escaped quote (`''` / `""`); otherwise it closes.
            if peek(bytes, i + 1) == Some(q) {
                s.push(q as char);
                i += 2;
                continue;
            }
            return Ok((s, i + 1));
        }
        if c == b'\\' && q == b'"' {
            match peek(bytes, i + 1) {
                Some(b'"') => {
                    s.push('"');
                    i += 2;
                    continue;
                }
                Some(b'\\') => {
                    s.push('\\');
                    i += 2;
                    continue;
                }
                _ => {}
            }
        }
        s.push(c as char);
        i += 1;
    }
    Err(LexError {
        msg: "unterminated string literal".into(),
        at: start,
    })
}

/// Try to lex an angle-bracketed IRI starting at `start` (the `<`) — CONCEPT:EG-KG.query.reason-iri-parses-angle.
/// Returns `Some((iri_with_brackets, next))` when `bytes[start..]` opens a whitespace-
/// free `<...>` run whose interior contains a `:` (a scheme separator); `None` otherwise
/// (so the caller falls back to the comparison `<`). Never consumes across whitespace or
/// a newline, so a stray `<` in `year < 2` stays the `Lt` operator.
fn lex_iri(bytes: &[u8], start: usize) -> Option<(String, usize)> {
    let mut i = start + 1;
    let mut has_colon = false;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'>' {
            // A closing `>` with a scheme colon inside and a non-empty body ⇒ an IRI.
            if has_colon && i > start + 1 {
                let iri = std::str::from_utf8(&bytes[start..=i]).ok()?.to_string();
                return Some((iri, i + 1));
            }
            return None;
        }
        if c.is_ascii_whitespace() {
            return None; // a comparison `<`, not an IRI
        }
        if c == b':' {
            has_colon = true;
        }
        i += 1;
    }
    None // unterminated `<...` ⇒ treat the `<` as the comparison operator
}

/// Lex a numeric literal (integer or decimal, optional leading `.`). Parsed as f64.
fn lex_number(src: &str, bytes: &[u8], start: usize) -> Result<(f64, usize), LexError> {
    let mut i = start;
    let mut seen_dot = false;
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_ascii_digit() {
            i += 1;
        } else if c == b'.' && !seen_dot && peek(bytes, i + 1) != Some(b'.') {
            // A single `.` is the decimal point; `..` is the range operator (stop).
            seen_dot = true;
            i += 1;
        } else {
            break;
        }
    }
    let text = &src[start..i];
    let n = text.parse::<f64>().map_err(|_| LexError {
        msg: format!("invalid number `{text}`"),
        at: start,
    })?;
    Ok((n, i))
}
