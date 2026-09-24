//! `RANK BY ~…` and `RERANK …`.

use eg_types::wire::Op;

use super::Parser;
use crate::uql::diag::UqlError;
use crate::uql::lexer::Tok;

impl<'a> Parser<'a> {
    /// `RANK BY ~ ( vector | string | param )`.
    pub(super) fn rank(&mut self) -> Result<Op, UqlError> {
        self.expect_kw("BY")?;
        self.expect(&Tok::Tilde, "`~` before the rank vector (`RANK BY ~[…]`)")?;
        if let Some(Tok::Str(_)) = self.peek_kind() {
            return Ok(Op::RankEmbed {
                text: self.string("a query text")?,
            });
        }
        if let Some(Tok::Ident(_)) = self.peek_kind() {
            return Err(self.err_here(
                "a bare-name embedding handle (`RANK BY ~handle`) is a reserved forward seam \
                 (no by-name embedding registry yet) — use `~'text'`, `~[…]` or `~$param`",
            ));
        }
        if let Some(eg_types::wire::UqlParam::Str(text)) = self.peek_str_param() {
            self.bump();
            return Ok(Op::RankEmbed { text });
        }
        Ok(Op::Rank {
            query: self.vector()?,
        })
    }

    /// A bound STRING parameter at the cursor (not consumed; marked used).
    fn peek_str_param(&mut self) -> Option<eg_types::wire::UqlParam> {
        let Some(Tok::Param(name)) = self.peek_kind() else {
            return None;
        };
        let value = self.params.get(name)?.clone();
        if !matches!(value, eg_types::wire::UqlParam::Str(_)) {
            return None;
        }
        self.used_params.insert(name.clone());
        Some(value)
    }

    /// `RERANK ( NODE_DISTANCE FROM id | MENTIONS | MMR num int )`.
    pub(super) fn rerank(&mut self) -> Result<Op, UqlError> {
        if self.eat_kw("MENTIONS") {
            return Ok(Op::RankMentions {});
        }
        if self.eat_kw("MMR") {
            let lambda = self.parse_number::<f32>("an MMR lambda")?;
            let k = self.parse_number::<usize>("an MMR k count")?;
            return Ok(Op::RankMmr { lambda, k });
        }
        if self.eat_kw("NODE_DISTANCE") {
            self.expect_kw("FROM")?;
            return Ok(Op::RankNodeDistance {
                center: self.id("a center node id")?,
            });
        }
        Err(self.err_here(
            "expected `NODE_DISTANCE FROM <id>`, `MENTIONS`, or `MMR <lambda> <k>` after RERANK",
        ))
    }
}
