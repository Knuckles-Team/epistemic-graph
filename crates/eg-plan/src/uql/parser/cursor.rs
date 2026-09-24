//! Token cursor: lookahead, consumption, expectations and nesting depth.

use super::{Parser, MAX_DEPTH};
use crate::uql::diag::{UqlCode, UqlError};
use crate::uql::lexer::Tok;

impl Parser<'_> {
    pub(super) fn peek_kind(&self) -> Option<&Tok> {
        self.toks.get(self.pos).map(|t| &t.kind)
    }

    pub(super) fn peek(&self, k: &Tok) -> bool {
        self.peek_kind() == Some(k)
    }

    /// Case-insensitive keyword lookahead (only a BARE word can be a keyword).
    pub(super) fn peek_kw(&self, kw: &str) -> bool {
        matches!(self.peek_kind(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw))
    }

    /// Keyword lookahead one token further.
    pub(super) fn peek_kw_at(&self, offset: usize, kw: &str) -> bool {
        matches!(
            self.toks.get(self.pos + offset).map(|t| &t.kind),
            Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw)
        )
    }

    pub(super) fn bump(&mut self) {
        self.pos += 1;
    }

    pub(super) fn eat(&mut self, k: &Tok) -> bool {
        let hit = self.peek(k);
        if hit {
            self.bump();
        }
        hit
    }

    pub(super) fn eat_kw(&mut self, kw: &str) -> bool {
        let hit = self.peek_kw(kw);
        if hit {
            self.bump();
        }
        hit
    }

    pub(super) fn expect(&mut self, k: &Tok, what: &str) -> Result<(), UqlError> {
        if self.eat(k) {
            return Ok(());
        }
        Err(self.err_here(&format!("expected {what}")))
    }

    pub(super) fn expect_kw(&mut self, kw: &str) -> Result<(), UqlError> {
        if self.eat_kw(kw) {
            return Ok(());
        }
        Err(self
            .err_here(&format!("expected keyword `{kw}`"))
            .expecting(vec![format!("`{kw}`")]))
    }

    /// Enter a nested construct (parentheses, `NOT`, a `FUSE` branch).
    pub(super) fn enter(&mut self) -> Result<(), UqlError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(self.error(
                UqlCode::NestingTooDeep,
                &format!("nesting deeper than {MAX_DEPTH} levels"),
            ));
        }
        Ok(())
    }

    pub(super) fn leave(&mut self) {
        self.depth -= 1;
    }

    pub(super) fn at_end(&self) -> bool {
        self.pos >= self.toks.len()
    }
}
