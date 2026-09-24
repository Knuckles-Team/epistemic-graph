//! DecideText tokens, lexed by THE UQL lexer (EH-452): one lexical family, so strings
//! (single- or double-quoted, escapes resolved), comments, Unicode words, IRIs and
//! `$name` parameters mean the same thing in both languages.
//!
//! The `{ … }` candidate query after `QUERY` is found by balancing brace TOKENS — a
//! string is one token, so a brace inside a string literal never counts — and its exact
//! source text is handed to the ordinary UQL parser, with its offset so the candidate's
//! own diagnostics point into the DecideText source.

use super::{DecideTextError, DecideTextErrorKind};
use crate::uql::lexer::{self as uql_lexer, Tok as UqlTok, Token};

/// One DecideText token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Tok {
    /// A keyword or a bare value: a word, an IRI (brackets stripped) or a number (its
    /// source text).
    Word(String),
    Str(String),
    /// `$name`.
    Param(String),
    /// A `{ … }` candidate query: its trimmed source text and the offset it starts at.
    Block(String, usize),
    LBracket,
    RBracket,
    Comma,
    Pipe,
}

/// A token and its byte span.
pub(super) type Spanned = (Tok, (usize, usize));

fn syntax(msg: impl Into<String>, span: (usize, usize)) -> DecideTextError {
    DecideTextError::new(DecideTextErrorKind::Syntax, msg, span)
}

/// A `$name` is DecideText's parameter; the old `@name` spelling is refused by name.
fn at_sign(span: (usize, usize)) -> DecideTextError {
    syntax("`@name` is not a DecideText parameter", span)
        .with_help("parameters are spelled `$name`, exactly as in UQL (`COVERS $capabilities`)")
}

/// Tokenise a DecideText source.
pub(super) fn lex(src: &str) -> Result<Vec<Spanned>, DecideTextError> {
    let tokens = uql_lexer::lex(src).map_err(|e| {
        let span = (e.at, e.at + 1);
        syntax(e.msg.clone(), span).caused_by(crate::uql::UqlError::from(e))
    })?;
    let mut out = Vec::with_capacity(tokens.len());
    let mut i = 0;
    while let Some(token) = tokens.get(i) {
        if token.kind == UqlTok::LBrace {
            let close = matching_brace(&tokens, i)?;
            out.push(block(src, token, &tokens[close]));
            i = close + 1;
            continue;
        }
        out.push((word_or_punct(src, token)?, (token.start, token.end)));
        i += 1;
    }
    Ok(out)
}

/// The index of the `}` token balancing the `{` at `open`.
fn matching_brace(tokens: &[Token], open: usize) -> Result<usize, DecideTextError> {
    let mut depth = 0usize;
    for (offset, token) in tokens[open..].iter().enumerate() {
        match token.kind {
            UqlTok::LBrace => depth += 1,
            UqlTok::RBrace => depth -= 1,
            _ => continue,
        }
        if depth == 0 {
            return Ok(open + offset);
        }
    }
    let at = tokens[open].start;
    Err(syntax(
        "unbalanced `{` in the candidate query",
        (at, at + 1),
    ))
}

fn block(src: &str, open: &Token, close: &Token) -> Spanned {
    let inner = &src[open.end..close.start];
    let offset = open.end + (inner.len() - inner.trim_start().len());
    (
        Tok::Block(inner.trim().to_string(), offset),
        (open.start, close.end),
    )
}

fn word_or_punct(src: &str, token: &Token) -> Result<Tok, DecideTextError> {
    let span = (token.start, token.end);
    Ok(match &token.kind {
        UqlTok::Ident(word) | UqlTok::QIdent(word) => Tok::Word(word.clone()),
        UqlTok::Iri(iri) => Tok::Word(iri.trim_start_matches('<').trim_end_matches('>').into()),
        UqlTok::Num(_) => Tok::Word(src[token.start..token.end].to_string()),
        UqlTok::Str(text) => Tok::Str(text.clone()),
        UqlTok::Param(name) => Tok::Param(name.clone()),
        UqlTok::LBracket => Tok::LBracket,
        UqlTok::RBracket => Tok::RBracket,
        UqlTok::Comma => Tok::Comma,
        UqlTok::Pipe => Tok::Pipe,
        UqlTok::At => return Err(at_sign(span)),
        other => return Err(syntax(format!("unexpected {other} in DecideText"), span)),
    })
}
