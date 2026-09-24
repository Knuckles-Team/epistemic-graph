//! `TRAVERSE` and `PROPAGATE`: edge patterns (direction, relationship, edge predicates),
//! hop ranges and the impact-model clause.

use eg_types::wire::{EdgeDir, Op, PropagateModel};

use super::Parser;
use crate::uql::diag::{UqlCode, UqlError};
use crate::uql::lexer::Tok;

/// `PROPAGATE` without `HOPS`.
const PROPAGATE_DEFAULT_HOPS: usize = 8;
/// `CASCADE` without `SAMPLES`.
const PROPAGATE_DEFAULT_SAMPLES: u32 = 2_000;

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

    /// `PROPAGATE model edge [HOPS n] [DEFAULT p]` (EH-526).
    pub(super) fn propagate(&mut self) -> Result<Op, UqlError> {
        let model = self.propagate_model()?;
        let (dir, rel, edge_preds) = self.edge_pattern()?;
        let hops = if self.eat_kw("HOPS") {
            self.parse_number::<usize>("a hop bound")?
        } else {
            PROPAGATE_DEFAULT_HOPS
        };
        let default_transmission = if self.eat_kw("DEFAULT") {
            self.transmission()?
        } else {
            1.0
        };
        Ok(Op::Propagate {
            model,
            rel,
            dir,
            edge_preds,
            hops,
            default_transmission,
        })
    }

    /// `NOISY_OR` | `CASCADE [SAMPLES n] [SEED s]`.
    fn propagate_model(&mut self) -> Result<PropagateModel, UqlError> {
        if self.eat_kw("NOISY_OR") {
            return Ok(PropagateModel::NoisyOr);
        }
        if !self.eat_kw("CASCADE") {
            return Err(self.err_here("expected `NOISY_OR` or `CASCADE` after PROPAGATE"));
        }
        let samples = if self.eat_kw("SAMPLES") {
            self.parse_number::<u32>("a sample count")?
        } else {
            PROPAGATE_DEFAULT_SAMPLES
        };
        let seed = if self.eat_kw("SEED") {
            self.parse_number::<u64>("a seed")?
        } else {
            0
        };
        Ok(PropagateModel::Cascade { samples, seed })
    }

    /// A default transmission probability in `[0, 1]`.
    fn transmission(&mut self) -> Result<f64, UqlError> {
        let start = self.cur_span();
        let p = self.number("a transmission probability")?;
        if !(0.0..=1.0).contains(&p) {
            return Err(UqlError::new(
                UqlCode::InvalidRange,
                "DEFAULT transmission must be in [0, 1]",
                (start.0, self.prev_span().1),
            ));
        }
        Ok(p)
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
