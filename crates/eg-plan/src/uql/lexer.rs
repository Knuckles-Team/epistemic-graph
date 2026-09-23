//! UQL lexer (CONCEPT:AU-KG.query.top-nodes-by-degree, UQL-02) — a hand-written,
//! dependency-free tokenizer over `char`s.
//!
//! Turns a UQL source string into a flat `Vec<Token>` with byte spans, so the
//! recursive-descent [`super::parser`] never touches raw characters. Pure Rust, no
//! regex / no DataFusion — the whole UQL front-end stays in the dep-free half of
//! eg-plan (the Pi contract): only EXECUTION is `query`-gated, parsing is not.
//!
//! Lexical grammar (the token half of `super::grammar`):
//!  * whitespace separates; `# …`, `-- …` and `// …` run to end of line (comments);
//!  * words are Unicode (`[\p{Alphabetic}_][\p{Alphanumeric}_]*`) — keywords are words
//!    the parser recognizes case-insensitively; `` `any text` `` is a QUOTED identifier
//!    (never a keyword; `` `` `` escapes a back-quote);
//!  * strings: `'…'` (`''` escapes) and `"…"` (`""`, `\"`, `\\` escape), UTF-8 intact;
//!  * numbers: `12`, `1.5`, `.5`, `1e-9`, `6.02E+23` (a leading `-` is a separate token;
//!    the parser folds it into signed literals); the token keeps its source span so the
//!    parser reads integers and `f32`s from the original text, exactly;
//!  * `$name` is a typed parameter; `$.a.b[0]` / `$[…]` is a JSONPath;
//!  * `<scheme:…>` (whitespace-free, with a `:`) is an IRI;
//!  * punctuation: `|>` `(` `)` `{` `}` `[` `]` `:` `,` `;` `*` `~` `@` `@>` `..`
//!    `->` `<-` `-` `=` `==` `!=` `<>` `>` `>=` `<` `<=`.

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
    /// A bare identifier / keyword. Keyword classification is done by the parser
    /// (case-insensitive compare), so the lexer stays context-free.
    Ident(String),
    /// A back-quoted identifier — a name, never a keyword.
    QIdent(String),
    /// A numeric literal (its exact text is the token's source span).
    Num(f64),
    /// A single- or double-quoted string literal (quotes stripped, escapes resolved).
    Str(String),
    /// An angle-bracketed IRI, stored WITH its brackets (`<http://ex/Device>`).
    Iri(String),
    /// `$name` — a typed parameter reference (the name, without `$`).
    Param(String),
    /// `$.a.b[0]` — a JSONPath, stored verbatim.
    Path(String),
    Pipe,
    LParen,
    RParen,
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    Colon,
    Comma,
    Semi,
    Star,
    Tilde,
    At,
    /// `@>` — JSON containment.
    AtGt,
    DotDot,
    /// `->`
    Arrow,
    /// `<-` (only before `[`: the start of an incoming edge pattern).
    LArrow,
    Dash,
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

impl fmt::Display for Tok {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Tok::Ident(s) => write!(f, "`{s}`"),
            Tok::QIdent(s) => write!(f, "quoted identifier `{s}`"),
            Tok::Num(n) => write!(f, "number `{n}`"),
            Tok::Str(s) => write!(f, "string \"{s}\""),
            Tok::Iri(s) => write!(f, "IRI `{s}`"),
            Tok::Param(s) => write!(f, "parameter `${s}`"),
            Tok::Path(s) => write!(f, "JSONPath `{s}`"),
            other => write!(f, "`{}`", punct_text(other)),
        }
    }
}

/// The spelling of a punctuation token (value tokens render through `Display`).
pub fn punct_text(t: &Tok) -> &'static str {
    PUNCT
        .iter()
        .find(|(_, k)| k == t)
        .map_or("?", |(text, _)| text)
}

/// Every punctuation spelling, longest first (so `|>` wins over `|`, `>=` over `>`).
const PUNCT: &[(&str, Tok)] = &[
    ("|>", Tok::Pipe),
    ("@>", Tok::AtGt),
    ("..", Tok::DotDot),
    ("->", Tok::Arrow),
    ("<-", Tok::LArrow),
    ("==", Tok::Eq),
    ("!=", Tok::Ne),
    ("<>", Tok::Ne),
    (">=", Tok::Ge),
    ("<=", Tok::Le),
    ("(", Tok::LParen),
    (")", Tok::RParen),
    ("{", Tok::LBrace),
    ("}", Tok::RBrace),
    ("[", Tok::LBracket),
    ("]", Tok::RBracket),
    (":", Tok::Colon),
    (",", Tok::Comma),
    (";", Tok::Semi),
    ("*", Tok::Star),
    ("~", Tok::Tilde),
    ("@", Tok::At),
    ("-", Tok::Dash),
    ("=", Tok::Eq),
    (">", Tok::Gt),
    ("<", Tok::Lt),
];

/// What went wrong while lexing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LexErrorKind {
    UnexpectedCharacter,
    UnterminatedString,
    UnterminatedIdentifier,
    InvalidNumber,
    EmptyParameterName,
}

/// A lexing error with the byte offset it occurred at.
#[derive(Clone, Debug, PartialEq)]
pub struct LexError {
    pub kind: LexErrorKind,
    pub msg: String,
    pub at: usize,
}

fn lex_err(kind: LexErrorKind, msg: impl Into<String>, at: usize) -> LexError {
    LexError {
        kind,
        msg: msg.into(),
        at,
    }
}

/// Tokenize a UQL source string.
pub fn lex(src: &str) -> Result<Vec<Token>, LexError> {
    let mut out = Vec::new();
    let mut i = 0;
    while let Some(c) = src[i..].chars().next() {
        if c.is_whitespace() {
            i += c.len_utf8();
            continue;
        }
        if starts_comment(&src[i..]) {
            i = src[i..].find('\n').map_or(src.len(), |n| i + n);
            continue;
        }
        let (kind, end) = lex_one(src, i, c)?;
        out.push(Token {
            kind,
            start: i,
            end,
        });
        i = end;
    }
    Ok(out)
}

fn starts_comment(rest: &str) -> bool {
    rest.starts_with('#') || rest.starts_with("--") || rest.starts_with("//")
}

type Lexed = Result<(Tok, usize), LexError>;

fn lex_one(src: &str, i: usize, c: char) -> Lexed {
    let rest = &src[i..];
    match c {
        '\'' | '"' => lex_string(src, i, c),
        '`' => lex_quoted_ident(src, i),
        '$' => lex_dollar(src, i),
        '<' => Ok(lex_angle(src, i)),
        '0'..='9' => lex_number(src, i),
        '.' if rest[1..].starts_with(|d: char| d.is_ascii_digit()) => lex_number(src, i),
        c if c.is_alphabetic() || c == '_' => {
            let end = scan(src, i, |c| c.is_alphanumeric() || c == '_');
            Ok((Tok::Ident(src[i..end].to_string()), end))
        }
        _ => lex_punct(rest, i),
    }
}

fn lex_punct(rest: &str, i: usize) -> Lexed {
    PUNCT
        .iter()
        .find(|(text, _)| rest.starts_with(text))
        .map(|(text, kind)| (kind.clone(), i + text.len()))
        .ok_or_else(|| {
            let c = rest.chars().next().unwrap_or(' ');
            lex_err(
                LexErrorKind::UnexpectedCharacter,
                format!("unexpected character `{c}`"),
                i,
            )
        })
}

fn scan(src: &str, from: usize, keep: impl Fn(char) -> bool) -> usize {
    src[from..]
        .char_indices()
        .find(|&(_, c)| !keep(c))
        .map_or(src.len(), |(n, _)| from + n)
}

/// `'…'` / `"…"`: a doubled quote escapes itself; in `"…"` also `\"` and `\\`.
fn lex_string(src: &str, start: usize, q: char) -> Lexed {
    let mut s = String::new();
    let mut chars = src[start + 1..].char_indices().peekable();
    while let Some((n, c)) = chars.next() {
        let next = chars.peek().map(|&(_, d)| d);
        if c == q && next == Some(q) {
            s.push(q);
            chars.next();
        } else if c == q {
            return Ok((Tok::Str(s), start + 1 + n + 1));
        } else if c == '\\' && q == '"' && matches!(next, Some('"') | Some('\\')) {
            s.push(next.unwrap_or('\\'));
            chars.next();
        } else {
            s.push(c);
        }
    }
    Err(lex_err(
        LexErrorKind::UnterminatedString,
        "unterminated string literal",
        start,
    ))
}

/// `` `name` ``: a doubled back-quote escapes itself.
fn lex_quoted_ident(src: &str, start: usize) -> Lexed {
    match lex_string(src, start, '`') {
        Ok((Tok::Str(s), end)) => Ok((Tok::QIdent(s), end)),
        Ok(other) => Ok(other),
        Err(_) => Err(lex_err(
            LexErrorKind::UnterminatedIdentifier,
            "unterminated back-quoted identifier",
            start,
        )),
    }
}

/// `$name` (parameter) or `$.a` / `$[0]` (JSONPath).
fn lex_dollar(src: &str, start: usize) -> Lexed {
    let after = &src[start + 1..];
    if after.starts_with('.') || after.starts_with('[') || after.is_empty() {
        let end = scan(src, start + 1, is_path_char);
        return Ok((Tok::Path(src[start..end].to_string()), end));
    }
    let end = scan(src, start + 1, |c| c.is_alphanumeric() || c == '_');
    if end == start + 1 {
        return Err(lex_err(
            LexErrorKind::EmptyParameterName,
            "`$` must be followed by a parameter name (`$name`) or a JSONPath (`$.field`)",
            start,
        ));
    }
    Ok((Tok::Param(src[start + 1..end].to_string()), end))
}

fn is_path_char(c: char) -> bool {
    c.is_alphanumeric() || "$._[]*".contains(c)
}

/// `<`: an IRI, `<-` before `[`, `<>`/`<=`, or the comparison `<`.
fn lex_angle(src: &str, start: usize) -> (Tok, usize) {
    let rest = &src[start..];
    if rest.starts_with("<-[") {
        return (Tok::LArrow, start + 2);
    }
    if let Some(end) = iri_end(rest) {
        return (Tok::Iri(rest[..end].to_string()), start + end);
    }
    match rest.as_bytes().get(1) {
        Some(b'>') => (Tok::Ne, start + 2),
        Some(b'=') => (Tok::Le, start + 2),
        _ => (Tok::Lt, start + 1),
    }
}

/// The byte length of a whitespace-free `<…>` run whose body holds a `:`.
fn iri_end(rest: &str) -> Option<usize> {
    let close = rest[1..].find(|c: char| c == '>' || c == '<' || c.is_whitespace())?;
    let body = &rest[1..1 + close];
    (rest[1 + close..].starts_with('>') && !body.is_empty() && body.contains(':'))
        .then_some(close + 2)
}

/// Digits, an optional fraction (a `..` range stops it), an optional exponent.
fn lex_number(src: &str, start: usize) -> Lexed {
    let mut end = scan(src, start, |c| c.is_ascii_digit());
    let rest = &src[end..];
    if rest.starts_with('.') && !rest.starts_with("..") {
        end = scan(src, end + 1, |c| c.is_ascii_digit());
    }
    end = exponent_end(src, end);
    let text = &src[start..end];
    let n = text.parse::<f64>().map_err(|_| {
        lex_err(
            LexErrorKind::InvalidNumber,
            format!("invalid number `{text}`"),
            start,
        )
    })?;
    if !n.is_finite() {
        return Err(lex_err(
            LexErrorKind::InvalidNumber,
            format!("number `{text}` is out of range"),
            start,
        ));
    }
    Ok((Tok::Num(n), end))
}

/// Extend past `e[+-]digits` when one follows; otherwise leave `end` unchanged (so
/// `3 e` is a number then a word).
fn exponent_end(src: &str, end: usize) -> usize {
    let rest = &src[end..];
    let Some(after_e) = rest.strip_prefix('e').or_else(|| rest.strip_prefix('E')) else {
        return end;
    };
    let sign = usize::from(after_e.starts_with('+') || after_e.starts_with('-'));
    let digits = after_e[sign..]
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(after_e.len() - sign);
    if digits == 0 {
        return end;
    }
    end + 1 + sign + digits
}
