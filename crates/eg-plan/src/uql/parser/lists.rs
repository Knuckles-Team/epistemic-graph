//! Bracketed lists, query vectors, list parameters and JSON literals.

use eg_types::wire::{PredLiteral, UqlParam};

use super::literal::Want;
use super::Parser;
use crate::uql::diag::{UqlCode, UqlError};
use crate::uql::lexer::Tok;

impl<'a> Parser<'a> {
    /// `"[" item { "," item } "]"` (possibly empty).
    pub(super) fn bracket_list<T>(
        &mut self,
        what: &str,
        mut item: impl FnMut(&mut Self) -> Result<T, UqlError>,
    ) -> Result<Vec<T>, UqlError> {
        self.expect(&Tok::LBracket, &format!("`[` to open {what}"))?;
        let mut out = Vec::new();
        if !self.peek(&Tok::RBracket) {
            loop {
                out.push(item(self)?);
                if !self.eat(&Tok::Comma) {
                    break;
                }
            }
        }
        self.expect(&Tok::RBracket, &format!("`]` to close {what}"))?;
        Ok(out)
    }

    /// A query vector: `[…]` of signed numbers, or a vector parameter.
    pub(super) fn vector(&mut self) -> Result<Vec<f32>, UqlError> {
        if self.peek(&Tok::LBracket) {
            return self.bracket_list("the vector", |p| {
                p.parse_number::<f32>("a vector component")
            });
        }
        match self.param(Want::Vector)? {
            Some(UqlParam::Vector(v)) => Ok(v),
            Some(_) => Err(self.param_type_error(Want::Vector)),
            None => Err(self.err_here("expected a vector (`[…]` or `$param`)")),
        }
    }

    /// `[ 'a', 'b' ]`.
    #[cfg(feature = "timeseries")]
    pub(super) fn string_list(&mut self, what: &str) -> Result<Vec<String>, UqlError> {
        self.bracket_list(what, |p| p.string("a list item"))
    }

    /// `[ 1, -2.5 ]`.
    #[cfg(any(feature = "geo", feature = "probabilistic"))]
    pub(super) fn number_list(&mut self, what: &str) -> Result<Vec<f64>, UqlError> {
        self.bracket_list(what, |p| p.number("a list item"))
    }

    /// A scalar list parameter (`x IN $values`).
    pub(super) fn list_param(&mut self) -> Result<Vec<PredLiteral>, UqlError> {
        match self.param(Want::List)? {
            Some(UqlParam::List(v)) => Ok(v),
            Some(_) => Err(self.param_type_error(Want::List)),
            None => Err(self.err_here("expected `(` or a list `$param` after IN")),
        }
    }

    /// A JSON literal: a scalar, `NULL`, or `JSON '<text>'`. Numbers are read from
    /// their source text by serde_json, so `3` stays an integer and `3.0` a float.
    pub(super) fn json_value(&mut self) -> Result<serde_json::Value, UqlError> {
        if self.eat_kw("NULL") {
            return Ok(serde_json::Value::Null);
        }
        if self.eat_kw("JSON") {
            let span = self.cur_span();
            let text = self.string("a JSON document")?;
            return serde_json::from_str(&text).map_err(|e| {
                UqlError::new(
                    UqlCode::UnexpectedToken,
                    format!("invalid JSON literal: {e}"),
                    span,
                )
            });
        }
        if let Some(b) = self.boolean() {
            return Ok(serde_json::Value::Bool(b));
        }
        let span = self.cur_span();
        if let Some(text) = self.signed_text() {
            return serde_json::from_str(&text).map_err(|_| {
                UqlError::new(
                    UqlCode::InvalidNumber,
                    format!("invalid number `{text}`"),
                    span,
                )
            });
        }
        Ok(match self.scalar()? {
            PredLiteral::Str(s) => serde_json::Value::String(s),
            PredLiteral::Num(n) => serde_json::Number::from_f64(n)
                .map_or(serde_json::Value::Null, serde_json::Value::Number),
            PredLiteral::Bool(b) => serde_json::Value::Bool(b),
        })
    }
}
