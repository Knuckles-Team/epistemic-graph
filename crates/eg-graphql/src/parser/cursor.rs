use super::error::GqlError;
use super::lexer::{Tok, Token};

pub(super) struct Cursor<'a> {
    toks: &'a [Token],
    pos: usize,
    end: usize,
}

impl<'a> Cursor<'a> {
    pub(super) fn new(toks: &'a [Token], end: usize) -> Self {
        Self { toks, pos: 0, end }
    }

    pub(super) fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos).map(|t| &t.kind)
    }
    pub(super) fn peek_is(&self, k: &Tok) -> bool {
        self.peek() == Some(k)
    }
    pub(super) fn bump(&mut self) {
        self.pos += 1;
    }
    pub(super) fn eat(&mut self, k: &Tok) -> bool {
        if self.peek_is(k) {
            self.bump();
            true
        } else {
            false
        }
    }
    pub(super) fn expect(&mut self, k: &Tok, what: &str) -> Result<(), GqlError> {
        if self.eat(k) {
            Ok(())
        } else {
            Err(self.err(&format!("expected {what}")))
        }
    }
    pub(super) fn expect_name(&mut self, what: &str) -> Result<String, GqlError> {
        match self.peek() {
            Some(Tok::Name(n)) => {
                let n = n.clone();
                self.bump();
                Ok(n)
            }
            _ => Err(self.err(&format!("expected {what}"))),
        }
    }
    pub(super) fn expect_eof(&mut self) -> Result<(), GqlError> {
        if self.pos >= self.toks.len() {
            Ok(())
        } else {
            Err(self.err("unexpected trailing tokens after the query"))
        }
    }
    pub(super) fn err(&self, msg: &str) -> GqlError {
        let at = self.toks.get(self.pos).map(|t| t.start).unwrap_or(self.end);
        GqlError {
            msg: msg.to_string(),
            at,
        }
    }
}
