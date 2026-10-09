//! The `VALIDATE SHAPE` stage (EH-196):
//!
//! ```text
//! validate_shape = "VALIDATE" "SHAPE" ( iri | string | ident ) [ "USING" string ]
//!                  [ "KEEP" ( "CONFORMING" | "VIOLATING" ) ] ;
//! ```
//!
//! → `Op::ValidateShape { shape, shapes, keep }`, `keep` defaulting to `CONFORMING`.
//! Without `USING` the shapes document is empty, which the executor resolves to the
//! queried graph's composed GraphSchema shapes (bound server-side; eg-uql made `USING`
//! optional once that binding existed).
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
        let shapes = if self.eat_kw("USING") {
            self.string("the shapes graph (a Turtle string) after `USING`")?
        } else {
            String::new()
        };
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
        Err(self.not_built("owl"))
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

    // spec: EG-FEDERATED-QUERY-R022, EG-FEDERATED-QUERY-R023, EG-FEDERATED-QUERY-R061, EG-FEDERATED-QUERY-R064, EG-FEDERATED-QUERY-R065, EG-FEDERATED-QUERY-R067
    #[test]
    fn using_is_optional_and_a_bad_keep_is_a_positioned_error() {
        // No USING ⇒ empty shapes ⇒ the graph's GraphSchema shapes (bound server-side).
        assert_eq!(
            stage("MATCH (:P) |> VALIDATE SHAPE <http://ex/S>"),
            Op::ValidateShape {
                shape: "<http://ex/S>".into(),
                shapes: String::new(),
                keep: ShapeKeep::Conforming,
            }
        );
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
