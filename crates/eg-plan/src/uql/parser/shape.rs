//! The `VALIDATE SHAPE` stage (EH-196):
//!
//! ```text
//! validate_shape = "VALIDATE" "SHAPE" ( iri | string | ident ) "USING" string
//!                  [ "KEEP" ( "CONFORMING" | "VIOLATING" ) ] ;
//! ```
//!
//! → `Op::ValidateShape { shape, shapes, keep }`, `keep` defaulting to `CONFORMING`.
//! Only `VALIDATE SHAPE` is a UQL stage; every other `VALIDATE …` clause belongs to the
//! DecideText front end, so this parser claims the stage only when BOTH keywords are
//! present and otherwise leaves the tokens untouched. Gated to `owl` (the feature that
//! compiles the variant and its SHACL executor), with the `REASON` precedent's clear
//! "not in this build" error elsewhere.

use eg_types::wire::Op;
#[cfg(feature = "owl")]
use eg_types::wire::ShapeKeep;

use super::{Parser, UqlError};
use crate::uql::lexer::Tok;

/// `KEEP` keywords and the rows each keeps.
#[cfg(feature = "owl")]
const KEEP_KEYWORDS: [(&str, ShapeKeep); 2] = [
    ("CONFORMING", ShapeKeep::Conforming),
    ("VIOLATING", ShapeKeep::Violating),
];

impl Parser<'_> {
    /// Parse a `VALIDATE SHAPE` stage when the next two tokens are those keywords.
    pub(super) fn parse_shape_stage(&mut self) -> Result<Option<Op>, UqlError> {
        if !(self.peek_kw("VALIDATE") && self.next_is_kw("SHAPE")) {
            return Ok(None);
        }
        self.bump();
        self.bump();
        self.parse_validate_shape().map(Some)
    }

    /// Case-insensitive keyword test on the token AFTER the current one.
    fn next_is_kw(&self, kw: &str) -> bool {
        matches!(
            self.toks.get(self.pos + 1).map(|token| &token.kind),
            Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw)
        )
    }

    #[cfg(feature = "owl")]
    fn parse_validate_shape(&mut self) -> Result<Op, UqlError> {
        let shape = match self.peek_kind() {
            Some(Tok::Iri(s)) | Some(Tok::Str(s)) | Some(Tok::Ident(s)) => s.clone(),
            _ => return Err(self.err_here("expected a shape IRI after `VALIDATE SHAPE`")),
        };
        self.bump();
        self.expect_kw("USING")?;
        let shapes = match self.peek_kind() {
            Some(Tok::Str(s)) => s.clone(),
            _ => {
                return Err(
                    self.err_here("expected the shapes graph as a Turtle string after `USING`")
                )
            }
        };
        self.bump();
        let keep = self.parse_shape_keep()?;
        Ok(Op::ValidateShape {
            shape,
            shapes,
            keep,
        })
    }

    /// The optional `KEEP CONFORMING | KEEP VIOLATING` suffix.
    #[cfg(feature = "owl")]
    fn parse_shape_keep(&mut self) -> Result<ShapeKeep, UqlError> {
        if !self.peek_kw("KEEP") {
            return Ok(ShapeKeep::Conforming);
        }
        self.bump();
        let Some((_, keep)) = KEEP_KEYWORDS.iter().find(|(kw, _)| self.peek_kw(kw)) else {
            return Err(self.err_here("expected `CONFORMING` or `VIOLATING` after `KEEP`"));
        };
        let keep = *keep;
        self.bump();
        Ok(keep)
    }

    /// `VALIDATE SHAPE` in a build WITHOUT `owl`: the variant is cfg'd out, so the
    /// clause is recognized but refused with a clear message.
    #[cfg(not(feature = "owl"))]
    fn parse_validate_shape(&mut self) -> Result<Op, UqlError> {
        Err(self.err_at(
            self.prev_start(),
            "`VALIDATE SHAPE` requires the SHACL engine (build feature `owl`/`owl-plan`); \
             not available in this build",
        ))
    }
}

#[cfg(all(test, feature = "owl"))]
mod tests {
    use eg_types::wire::{Op, ShapeKeep};

    use crate::uql::parse;

    fn stage(src: &str) -> Op {
        parse(src).expect("parses").ops.pop().expect("a stage")
    }

    #[test]
    fn validate_shape_lowers_to_one_op_with_conforming_by_default() {
        let op = stage(r#"MATCH (:Person) |> VALIDATE SHAPE <http://ex/Named> USING 'ttl'"#);
        assert_eq!(
            op,
            Op::ValidateShape {
                shape: "<http://ex/Named>".into(),
                shapes: "ttl".into(),
                keep: ShapeKeep::Conforming,
            }
        );
    }

    #[test]
    fn keep_violating_is_parsed_case_insensitively() {
        let op = stage(
            r#"MATCH (:Person) |> validate shape "http://ex/Named" using "t" keep violating"#,
        );
        assert!(matches!(
            op,
            Op::ValidateShape {
                keep: ShapeKeep::Violating,
                ..
            }
        ));
    }

    #[test]
    fn a_missing_using_clause_or_bad_keep_is_a_positioned_error() {
        let no_using = parse("MATCH (:P) |> VALIDATE SHAPE <http://ex/S>").unwrap_err();
        assert!(no_using.msg.contains("USING"), "{}", no_using.msg);
        let bad_keep =
            parse("MATCH (:P) |> VALIDATE SHAPE <http://ex/S> USING 't' KEEP ALL").unwrap_err();
        assert!(bad_keep.msg.contains("CONFORMING"), "{}", bad_keep.msg);
    }

    #[test]
    fn other_validate_clauses_are_left_to_the_decision_front_end() {
        let error = parse("MATCH (:P) |> VALIDATE POLICY DEFAULT").unwrap_err();
        assert!(!error.msg.contains("shape IRI"), "{}", error.msg);
    }
}
