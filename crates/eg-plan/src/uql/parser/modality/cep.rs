//! `CEP` pattern syntax (EH-368, `stream`):
//! ```text
//! cep      = "CEP" cep_node "WINDOW" ( "SLIDING" | "TUMBLING" ) int ;
//! cep_node = "SEQ" "(" [ matcher { "," matcher } ] ")"
//!          | "WITHIN" int "(" cep_node ")"
//!          | "ABSENCE" matcher "THEN" "NOT" matcher "WITHIN" int ;
//! matcher  = "{" [ "KEY" string ] [ "WHERE" cep_pred { "AND" cep_pred } ] "}" ;
//! cep_pred = name ( "=" json | ">" signed_num | "<" signed_num | "EXISTS" ) ;
//! ```

use eg_types::wire::{CepAttrPredSpec, CepMatcherSpec, CepNodeSpec, CepPatternSpec, CepWindowSpec};

use crate::uql::diag::UqlError;
use crate::uql::lexer::Tok;
use crate::uql::parser::Parser;

impl<'a> Parser<'a> {
    /// The pattern and window after `CEP`.
    pub(in crate::uql::parser) fn cep_pattern(&mut self) -> Result<CepPatternSpec, UqlError> {
        let pattern = self.cep_node()?;
        self.expect_kw("WINDOW")?;
        let window = if self.eat_kw("SLIDING") {
            CepWindowSpec::Sliding {
                size: self.parse_number::<u64>("a window size")?,
            }
        } else {
            self.expect_kw("TUMBLING")?;
            CepWindowSpec::Tumbling {
                size: self.parse_number::<u64>("a window size")?,
            }
        };
        Ok(CepPatternSpec { pattern, window })
    }

    fn cep_node(&mut self) -> Result<CepNodeSpec, UqlError> {
        if self.eat_kw("SEQ") {
            self.expect(&Tok::LParen, "`(` after SEQ")?;
            let mut matchers = Vec::new();
            if !self.peek(&Tok::RParen) {
                matchers.push(self.cep_matcher()?);
                while self.eat(&Tok::Comma) {
                    matchers.push(self.cep_matcher()?);
                }
            }
            self.expect(&Tok::RParen, "`)` to close SEQ")?;
            return Ok(CepNodeSpec::Sequence(matchers));
        }
        if self.eat_kw("WITHIN") {
            let within = self.parse_number::<u64>("a duration")?;
            self.expect(&Tok::LParen, "`(` after WITHIN n")?;
            self.enter()?;
            let inner = self.cep_node()?;
            self.leave();
            self.expect(&Tok::RParen, "`)` to close WITHIN")?;
            return Ok(CepNodeSpec::Within {
                within,
                pattern: Box::new(inner),
            });
        }
        self.expect_kw("ABSENCE")?;
        let a = self.cep_matcher()?;
        self.expect_kw("THEN")?;
        self.expect_kw("NOT")?;
        let b = self.cep_matcher()?;
        self.expect_kw("WITHIN")?;
        Ok(CepNodeSpec::Absence {
            a,
            b,
            within: self.parse_number::<u64>("a duration")?,
        })
    }

    fn cep_matcher(&mut self) -> Result<CepMatcherSpec, UqlError> {
        self.expect(&Tok::LBrace, "`{` to open an event matcher")?;
        let key = if self.eat_kw("KEY") {
            Some(self.string("an event key")?)
        } else {
            None
        };
        let mut preds = Vec::new();
        if self.eat_kw("WHERE") {
            preds.push(self.cep_pred()?);
            while self.eat_kw("AND") {
                preds.push(self.cep_pred()?);
            }
        }
        self.expect(&Tok::RBrace, "`}` to close the event matcher")?;
        Ok(CepMatcherSpec { key, preds })
    }

    fn cep_pred(&mut self) -> Result<CepAttrPredSpec, UqlError> {
        let field = self.name("an event attribute")?;
        if self.eat_kw("EXISTS") {
            return Ok(CepAttrPredSpec::Exists { field });
        }
        if self.eat(&Tok::Eq) {
            return Ok(CepAttrPredSpec::Eq {
                field,
                value: self.json_value()?,
            });
        }
        if self.eat(&Tok::Gt) {
            return Ok(CepAttrPredSpec::Gt {
                field,
                value: self.number("a number")?,
            });
        }
        self.expect(&Tok::Lt, "`=`, `>`, `<` or `EXISTS`")?;
        Ok(CepAttrPredSpec::Lt {
            field,
            value: self.number("a number")?,
        })
    }
}
