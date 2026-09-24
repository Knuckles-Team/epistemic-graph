//! `TRAVERSE`: edge patterns (direction, relationship, edge predicates) and hop ranges.

use eg_types::wire::{EdgeDir, Op};

use super::Parser;
use crate::uql::diag::{UqlCode, UqlError};
use crate::uql::lexer::Tok;

impl<'a> Parser<'a> {
    /// `TRAVERSE edge [hops]`.
    pub(super) fn traverse(&mut self) -> Result<Op, UqlError> {
        let (dir, rel, edge_preds) = self.edge_pattern()?;
        let (min, max) = self.hop_range()?;
        Ok(match (dir, rel, edge_preds.is_empty()) {
            (EdgeDir::Out, Some(rel), true) => Op::Traverse { rel, min, max },
            (dir, rel, _) => Op::Expand {
                rel,
                dir,
                min,
                max,
                edge_preds,
            },
        })
    }

    /// `-[rel]->` | `-[rel]-` | `<-[rel]-` | bare `name` (outgoing).
    fn edge_pattern(
        &mut self,
    ) -> Result<(EdgeDir, Option<String>, Vec<eg_types::wire::Pred>), UqlError> {
        let incoming = self.eat(&Tok::LArrow);
        if !incoming && !self.eat(&Tok::Dash) {
            let rel = self.name("a relationship type (or `-[:REL]->`)")?;
            return Ok((EdgeDir::Out, Some(rel), Vec::new()));
        }
        self.expect(&Tok::LBracket, "`[` in the edge pattern")?;
        let rel = if self.eat(&Tok::Star) {
            None
        } else {
            self.expect(&Tok::Colon, "`:REL` or `*` in the edge pattern")?;
            Some(self.name("a relationship type")?)
        };
        let preds = if self.eat_kw("WHERE") {
            self.pred_list()?
        } else {
            Vec::new()
        };
        self.expect(&Tok::RBracket, "`]` to close the edge pattern")?;
        let dir = self.edge_close(incoming)?;
        Ok((dir, rel, preds))
    }

    /// The closing `->` / `-` of an edge pattern.
    fn edge_close(&mut self, incoming: bool) -> Result<EdgeDir, UqlError> {
        if incoming {
            self.expect(&Tok::Dash, "`-` to close `<-[…]-`")?;
            return Ok(EdgeDir::In);
        }
        if self.eat(&Tok::Arrow) {
            return Ok(EdgeDir::Out);
        }
        self.expect(&Tok::Dash, "`->` (outgoing) or `-` (either direction)")?;
        Ok(EdgeDir::Both)
    }

    /// `hops = { int [ (, | ..) int ] }`; absent ⇒ exactly one hop.
    fn hop_range(&mut self) -> Result<(usize, usize), UqlError> {
        if !self.eat(&Tok::LBrace) {
            return Ok((1, 1));
        }
        let start = self.prev_start();
        let min = self.parse_number::<usize>("a minimum hop count")?;
        let max = if self.eat(&Tok::Comma) || self.eat(&Tok::DotDot) {
            self.parse_number::<usize>("a maximum hop count")?
        } else {
            min
        };
        self.expect(&Tok::RBrace, "`}` to close the hop range")?;
        if max < min {
            return Err(UqlError::new(
                UqlCode::InvalidRange,
                "hop range max must be ≥ min",
                (start, self.prev_span().1),
            ));
        }
        Ok((min, max))
    }
}
