use super::error::GqlError;

// ── lexer ───────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub(super) enum Tok {
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
pub(super) struct Token {
    pub(super) kind: Tok,
    pub(super) start: usize,
}

pub(super) fn lex(src: &str) -> Result<Vec<Token>, GqlError> {
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
            skip_comment(b, &mut i);
            continue;
        }
        out.push(lex_token(src, b, &mut i)?);
    }
    Ok(out)
}

fn skip_comment(b: &[u8], i: &mut usize) {
    while *i < b.len() && b[*i] != b'\n' {
        *i += 1;
    }
}

fn lex_token(src: &str, b: &[u8], i: &mut usize) -> Result<Token, GqlError> {
    let start = *i;
    let c = b[*i];
    let (kind, next) = if let Some(kind) = lex_symbol(c) {
        (kind, *i + 1)
    } else {
        match c {
            b'.' => {
                // The only `.`-led token is the three-dot spread `...`.
                match spread_end(b, *i) {
                    Some(next) => (Tok::Spread, next),
                    None => {
                        return Err(GqlError {
                            msg: "expected `...` (a spread)".into(),
                            at: start,
                        })
                    }
                }
            }
            b'"' => {
                let (s, next) = lex_str(b, *i)?;
                (Tok::Str(s), next)
            }
            b'-' | b'0'..=b'9' => lex_num(src, b, *i)?,
            _ if is_name_start(c) => {
                let end = name_end(b, *i);
                (Tok::Name(src[start..end].to_string()), end)
            }
            _ => {
                return Err(GqlError {
                    msg: format!("unexpected character `{}`", c as char),
                    at: start,
                })
            }
        }
    };
    *i = next;
    Ok(Token { kind, start })
}

fn spread_end(b: &[u8], start: usize) -> Option<usize> {
    (start + 2 < b.len() && b[start + 1] == b'.' && b[start + 2] == b'.').then_some(start + 3)
}

fn name_end(b: &[u8], start: usize) -> usize {
    let mut end = start + 1;
    while end < b.len() && is_name_continue(b[end]) {
        end += 1;
    }
    end
}

fn lex_symbol(c: u8) -> Option<Tok> {
    const SYMBOLS: &[(u8, Tok)] = &[
        (b'{', Tok::LBrace),
        (b'}', Tok::RBrace),
        (b'(', Tok::LParen),
        (b')', Tok::RParen),
        (b'[', Tok::LBracket),
        (b']', Tok::RBracket),
        (b':', Tok::Colon),
        (b',', Tok::Comma),
        (b'!', Tok::Bang),
        (b'$', Tok::Dollar),
        (b'@', Tok::At),
        (b'=', Tok::Eq),
    ];
    SYMBOLS
        .iter()
        .find(|(symbol, _)| *symbol == c)
        .map(|(_, kind)| kind.clone())
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
    let (i, is_float) = scan_number(b, start);
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

fn scan_number(b: &[u8], start: usize) -> (usize, bool) {
    let mut i = start;
    if b[i] == b'-' {
        i += 1;
    }
    let mut is_float = false;
    while i < b.len() {
        let Some(float_part) = number_part(b[i], i == start) else {
            break;
        };
        is_float |= float_part;
        i += 1;
    }
    (i, is_float)
}

fn number_part(c: u8, first: bool) -> Option<bool> {
    if c.is_ascii_digit() {
        Some(false)
    } else if first {
        None
    } else {
        match c {
            b'.' | b'e' | b'E' => Some(true),
            b'+' | b'-' => Some(false),
            _ => None,
        }
    }
}
