//! The always-available clauses: `MATCH`, `WHERE`, `TRAVERSE`, `RANK`, `RERANK`,
//! `AS OF`/`VALID AS OF`, `WINDOW`, `LIMIT`, `RETURN` and `FOREIGN`.

use eg_types::wire::{EdgeDir, Op, TimeAxis};

use super::Parser;
use crate::uql::diag::{UqlCode, UqlError};
use crate::uql::lexer::Tok;

/// `WINDOW` units → seconds, as (spellings, numerator, denominator): `secs = n * num / den`
/// (a sub-second unit DIVIDES, so `5 ms` is the correctly-rounded `0.005`).
const UNITS: &[(&[&str], f64, f64)] = &[
    (&["ns", "nanos"], 1.0, 1e9),
    (&["us", "micros"], 1.0, 1e6),
    (&["ms", "millis"], 1.0, 1e3),
    (&["s", "sec", "secs"], 1.0, 1.0),
    (&["m", "min", "mins"], 60.0, 1.0),
    (&["h", "hr", "hrs"], 3_600.0, 1.0),
    (&["d", "day", "days"], 86_400.0, 1.0),
];

/// `WINDOW` aggregate spellings → the executor's canonical names.
const AGGREGATES: &[(&[&str], &str)] = &[
    (&["mean", "avg", "average"], "mean"),
    (&["sum"], "sum"),
    (&["min", "minimum"], "min"),
    (&["max", "maximum"], "max"),
    (&["count"], "count"),
    (&["first"], "first"),
    (&["last"], "last"),
];

impl<'a> Parser<'a> {
    /// `MATCH ( [ : name ] ) [ WHERE pred ]` — `MATCH` consumed.
    pub(super) fn match_source(&mut self) -> Result<Vec<Op>, UqlError> {
        self.expect(&Tok::LParen, "`(` after MATCH")?;
        let head = if self.peek(&Tok::RParen) {
            Op::ScanAll {}
        } else {
            let _ = self.eat(&Tok::Colon);
            Op::Scan {
                label: self.name("a node label")?,
            }
        };
        self.expect(&Tok::RParen, "`)` to close the MATCH pattern")?;
        let mut ops = vec![head];
        if self.eat_kw("WHERE") {
            ops.push(Op::Filter {
                preds: self.pred_list()?,
            });
        }
        Ok(ops)
    }

    pub(super) fn where_stage(&mut self) -> Result<Op, UqlError> {
        Ok(Op::Filter {
            preds: self.pred_list()?,
        })
    }

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

    /// `ts = @ signed_num | param`.
    pub(super) fn timestamp(&mut self) -> Result<f64, UqlError> {
        if self.eat(&Tok::At) {
            return self.number("a timestamp (unix seconds)");
        }
        if matches!(self.peek_kind(), Some(Tok::Param(_))) {
            return self.number("a timestamp (unix seconds)");
        }
        Err(self.err_here("expected `@<unix seconds>` or a `$param` timestamp"))
    }

    /// `AS OF [TX | VALID] ts`.
    pub(super) fn as_of(&mut self) -> Result<Op, UqlError> {
        self.expect_kw("OF")?;
        let axis = if self.eat_kw("TX") || self.eat_kw("TRANSACTION") {
            TimeAxis::Transaction
        } else {
            let _ = self.eat_kw("VALID");
            TimeAxis::Valid
        };
        Ok(Op::AsOf {
            ts: self.timestamp()?,
            axis,
        })
    }

    /// `VALID AS OF ts` — an alias of `AS OF VALID ts` (always available).
    pub(super) fn valid_as_of(&mut self) -> Result<Op, UqlError> {
        self.expect_kw("AS")?;
        self.expect_kw("OF")?;
        Ok(Op::AsOf {
            ts: self.timestamp()?,
            axis: TimeAxis::Valid,
        })
    }

    /// `WINDOW num [unit] [agg]`.
    pub(super) fn window(&mut self) -> Result<Op, UqlError> {
        let n = self.number("a WINDOW duration")?;
        let secs = match self.lookup_word(UNITS.iter().map(|(names, _, _)| *names)) {
            Some(i) => n * UNITS[i].1 / UNITS[i].2,
            None => n,
        };
        if !secs.is_finite() {
            return Err(self.err_at(self.prev_start(), "the WINDOW duration overflows"));
        }
        match self.lookup_word(AGGREGATES.iter().map(|(names, _)| *names)) {
            Some(i) => Ok(Op::WindowAgg {
                secs,
                agg: AGGREGATES[i].1.to_string(),
            }),
            None => Ok(Op::Window { secs }),
        }
    }

    /// Consume the next word when it is one of a table row's spellings; its row index.
    fn lookup_word<'t>(&mut self, rows: impl Iterator<Item = &'t [&'t str]>) -> Option<usize> {
        let Some(Tok::Ident(word)) = self.peek_kind() else {
            return None;
        };
        let word = word.to_ascii_lowercase();
        let hit = rows
            .enumerate()
            .find(|(_, names)| names.contains(&word.as_str()))?;
        self.bump();
        Some(hit.0)
    }

    /// `LIMIT (int | param)`.
    pub(super) fn limit(&mut self) -> Result<Op, UqlError> {
        Ok(Op::Limit {
            k: self.parse_number::<usize>("a LIMIT count")?,
        })
    }

    /// `RETURN name {, name}`.
    pub(super) fn return_stage(&mut self) -> Result<Op, UqlError> {
        let mut channels = vec![self.name("a score channel")?];
        while self.eat(&Tok::Comma) {
            channels.push(self.name("a score channel")?);
        }
        Ok(Op::Project { channels })
    }

    /// `FOREIGN id` | `FOREIGN SCAN …` | `FOREIGN HTTP …` (federation).
    pub(super) fn foreign(&mut self) -> Result<Op, UqlError> {
        if self.peek_kw("SCAN") || self.peek_kw("HTTP") {
            return self.foreign_scan();
        }
        if self.peek_kw("ENGINE") || self.peek_kw("SQL") {
            return Err(self
                .error(
                    UqlCode::CredentialBearingSpec,
                    "an inline remote-engine / SQL foreign source carries credentials and \
                     cannot be written in query text",
                )
                .with_help(
                    "register it with RegisterForeignSource, then `FOREIGN SCAN '<name>'`",
                ));
        }
        Ok(Op::Foreign {
            name: self.id("a FOREIGN source name")?,
        })
    }
}
