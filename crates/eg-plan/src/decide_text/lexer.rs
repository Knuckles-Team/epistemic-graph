//! DecideText tokens. Words are case-insensitive keywords or identifiers; a
//! `{ ... }` block after `QUERY` is captured raw (balanced) and handed to the
//! ordinary UQL parser, so `|>` inside it belongs to the candidate query and
//! never to the decision clauses.

use super::{DecideTextError, DecideTextErrorKind};

/// One token and the byte offset it starts at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Tok {
    Word(String),
    Str(String),
    Param(String),
    Block(String),
    LBracket,
    RBracket,
    Comma,
    Pipe,
}

pub(super) type Spanned = (Tok, usize);

fn syntax(msg: impl Into<String>, at: usize) -> DecideTextError {
    DecideTextError::new(DecideTextErrorKind::Syntax, msg, at)
}

fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '_' | ':' | '/' | '-' | '.' | '#')
}

fn take_while(src: &str, start: usize, keep: impl Fn(char) -> bool) -> usize {
    src[start..]
        .char_indices()
        .find(|&(_, c)| !keep(c))
        .map_or(src.len(), |(offset, _)| start + offset)
}

fn string(src: &str, at: usize) -> Result<(Tok, usize), DecideTextError> {
    let close = src[at + 1..]
        .find('"')
        .ok_or_else(|| syntax("unterminated string", at))?;
    let end = at + 1 + close;
    Ok((Tok::Str(src[at + 1..end].to_string()), end + 1))
}

fn block(src: &str, at: usize) -> Result<(Tok, usize), DecideTextError> {
    let mut depth = 0usize;
    for (offset, c) in src[at..].char_indices() {
        depth = match c {
            '{' => depth + 1,
            '}' => depth - 1,
            _ => depth,
        };
        if depth == 0 {
            let end = at + offset;
            return Ok((Tok::Block(src[at + 1..end].trim().to_string()), end + 1));
        }
    }
    Err(syntax("unbalanced `{` in the candidate query", at))
}

fn param(src: &str, at: usize) -> Result<(Tok, usize), DecideTextError> {
    let end = take_while(src, at + 1, |c| c.is_alphanumeric() || c == '_');
    if end == at + 1 {
        return Err(syntax("`@` must name a parameter", at));
    }
    Ok((Tok::Param(src[at + 1..end].to_string()), end))
}

fn punctuation(c: char) -> Option<Tok> {
    match c {
        '[' => Some(Tok::LBracket),
        ']' => Some(Tok::RBracket),
        ',' => Some(Tok::Comma),
        _ => None,
    }
}

fn next_token(src: &str, at: usize, c: char) -> Result<(Tok, usize), DecideTextError> {
    if let Some(tok) = punctuation(c) {
        return Ok((tok, at + 1));
    }
    match c {
        '"' => string(src, at),
        '{' => block(src, at),
        '@' => param(src, at),
        '|' if src[at..].starts_with("|>") => Ok((Tok::Pipe, at + 2)),
        c if is_word_char(c) => {
            let end = take_while(src, at, is_word_char);
            Ok((Tok::Word(src[at..end].to_string()), end))
        }
        other => Err(syntax(format!("unexpected character `{other}`"), at)),
    }
}

/// Tokenise a DecideText source.
pub(super) fn lex(src: &str) -> Result<Vec<Spanned>, DecideTextError> {
    let mut tokens = Vec::new();
    let mut at = 0;
    while let Some(c) = src[at..].chars().next() {
        if c.is_whitespace() {
            at += c.len_utf8();
            continue;
        }
        let (tok, end) = next_token(src, at, c)?;
        tokens.push((tok, at));
        at = end;
    }
    Ok(tokens)
}
