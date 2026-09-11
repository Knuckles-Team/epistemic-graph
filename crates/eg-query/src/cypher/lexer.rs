//! The Cypher subset's tokenizer (CONCEPT:EG-KG.query.dep-free-behind).
//!
//! Dep-free — a char scanner over token *classes* held as data: punctuation and the
//! one-or-two-character operators are tables, and each variable-length class (string,
//! number, `$param`, identifier) is one scanning phase. [`super::parser`] owns the
//! grammar; nothing here knows what a token means.

/// A flat token. The tokenizer is whitespace-insensitive; punctuation is matched
/// greedily for the multi-char operators (`->`, `<-`, `<=`, `>=`, `<>`, `!=`,
/// `..`, `*`).
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Tok {
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Colon,
    Dot,
    Comma,
    Star,
    DotDot,
    Dash,       // '-'
    ArrowRight, // '->'
    ArrowLeft,  // '<-'
    Eq,
    Ne, // '<>' or '!='
    Lt,
    Le,
    Gt,
    Ge,
    Ident(String),
    Str(String),
    Num(f64),
    /// `$name` — a query parameter reference (CONCEPT:EG-KG.query.param-list-drives-unwind).
    Param(String),
}

/// Characters that are a whole token on their own.
static PUNCTUATION: [(char, Tok); 10] = [
    ('(', Tok::LParen),
    (')', Tok::RParen),
    ('[', Tok::LBracket),
    (']', Tok::RBracket),
    ('{', Tok::LBrace),
    ('}', Tok::RBrace),
    (':', Tok::Colon),
    (',', Tok::Comma),
    ('*', Tok::Star),
    ('=', Tok::Eq),
];

/// A greedy one-or-two-character operator: `lead` followed by one of `pairs` is that
/// two-character token, otherwise `lead` alone is `single` — and a `single` of `None`
/// makes the lone character an error.
struct Operator {
    lead: char,
    pairs: &'static [(char, Tok)],
    single: Option<Tok>,
}

static OPERATORS: [Operator; 5] = [
    Operator {
        lead: '.',
        pairs: &[('.', Tok::DotDot)],
        single: Some(Tok::Dot),
    },
    Operator {
        lead: '-',
        pairs: &[('>', Tok::ArrowRight)],
        single: Some(Tok::Dash),
    },
    Operator {
        lead: '<',
        pairs: &[('-', Tok::ArrowLeft), ('=', Tok::Le), ('>', Tok::Ne)],
        single: Some(Tok::Lt),
    },
    Operator {
        lead: '>',
        pairs: &[('=', Tok::Ge)],
        single: Some(Tok::Gt),
    },
    Operator {
        lead: '!',
        pairs: &[('=', Tok::Ne)],
        single: None,
    },
];

/// The token an operator lead character produces, with how many characters it consumed.
fn operator_token(op: &Operator, next: Option<char>) -> Result<(Tok, usize), String> {
    if let Some(n) = next {
        if let Some((_, tok)) = op.pairs.iter().find(|(c, _)| *c == n) {
            return Ok((tok.clone(), 2));
        }
    }
    match &op.single {
        Some(tok) => Ok((tok.clone(), 1)),
        None => Err(format!("unexpected {:?}", op.lead)),
    }
}

/// A quoted string literal. `open` indexes the opening quote; the returned width spans
/// through the closing quote. Minimal escape handling: `\x` passes `x` through.
fn scan_string(chars: &[char], open: usize) -> Result<(Tok, usize), String> {
    let quote = chars[open];
    let mut i = open + 1;
    let mut s = String::new();
    while i < chars.len() && chars[i] != quote {
        if chars[i] == '\\' && i + 1 < chars.len() {
            s.push(chars[i + 1]);
            i += 2;
        } else {
            s.push(chars[i]);
            i += 1;
        }
    }
    if i >= chars.len() {
        return Err("unterminated string literal".into());
    }
    Ok((Tok::Str(s), i + 1 - open))
}

/// An unsigned numeric literal. `-` is always a `Dash` token — the grammar has no
/// negative literals in this subset — and a `..` range token ends the number.
fn scan_number(chars: &[char], start: usize) -> Result<(Tok, usize), String> {
    let mut i = start;
    while i < chars.len()
        && (chars[i].is_ascii_digit() || chars[i] == '.')
        && !(chars[i] == '.' && i + 1 < chars.len() && chars[i + 1] == '.')
    {
        i += 1;
    }
    let num_str: String = chars[start..i].iter().collect();
    let n: f64 = num_str
        .parse()
        .map_err(|_| format!("bad number: {num_str}"))?;
    Ok((Tok::Num(n), i - start))
}

/// The run of identifier characters at `start`.
fn word_end(chars: &[char], start: usize) -> usize {
    let mut i = start;
    while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
        i += 1;
    }
    i
}

/// A `$name` parameter reference (CONCEPT:EG-KG.query.param-list-drives-unwind);
/// `dollar` indexes the `$`.
fn scan_param(chars: &[char], dollar: usize) -> Result<(Tok, usize), String> {
    let start = dollar + 1;
    let end = word_end(chars, start);
    if end == start {
        return Err("expected a parameter name after '$'".into());
    }
    Ok((Tok::Param(chars[start..end].iter().collect()), end - dollar))
}

/// An identifier or keyword; keyword recognition is the grammar's job.
fn scan_ident(chars: &[char], start: usize) -> (Tok, usize) {
    let end = word_end(chars, start);
    (Tok::Ident(chars[start..end].iter().collect()), end - start)
}

/// The token class at `chars[i]`, with how many characters it consumed. `None` for
/// whitespace, which is not a token.
fn next_token(chars: &[char], i: usize) -> Result<Option<(Tok, usize)>, String> {
    let c = chars[i];
    if c.is_whitespace() {
        return Ok(None);
    }
    if let Some((_, tok)) = PUNCTUATION.iter().find(|(ch, _)| *ch == c) {
        return Ok(Some((tok.clone(), 1)));
    }
    if let Some(op) = OPERATORS.iter().find(|op| op.lead == c) {
        return operator_token(op, chars.get(i + 1).copied()).map(Some);
    }
    // The variable-length classes, in the order the grammar disambiguates them.
    let scanned = if c == '\'' || c == '"' {
        scan_string(chars, i)
    } else if c.is_ascii_digit() {
        scan_number(chars, i)
    } else if c == '$' {
        scan_param(chars, i)
    } else if c.is_alphabetic() || c == '_' {
        Ok(scan_ident(chars, i))
    } else {
        Err(format!("unexpected character: {c:?}"))
    };
    scanned.map(Some)
}

pub(super) fn tokenize(input: &str) -> Result<Vec<Tok>, String> {
    let chars: Vec<char> = input.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        match next_token(&chars, i)? {
            Some((tok, width)) => {
                out.push(tok);
                i += width;
            }
            None => i += 1,
        }
    }
    Ok(out)
}
