//! The always-available clauses: `MATCH`, `WHERE`, `TRAVERSE`, `RANK`, `RERANK`,
//! `AS OF`/`VALID AS OF`, `WINDOW`, `LIMIT`, `RETURN`, `FOREIGN` and `DECISIONS`.

use eg_types::wire::{Op, TimeAxis};

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

    /// `DECISIONS [ WHERE pred ]` (EH-066) — the caller's visible decision log.
    pub(super) fn decisions(&mut self) -> Result<Op, UqlError> {
        let preds = if self.eat_kw("WHERE") {
            self.pred_list()?
        } else {
            Vec::new()
        };
        Ok(Op::DecisionScan { preds })
    }

    pub(super) fn where_stage(&mut self) -> Result<Op, UqlError> {
        Ok(Op::Filter {
            preds: self.pred_list()?,
        })
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

    /// `RETURN name {, name}` — each name a score channel this build produces, a series
    /// value channel `v0..vk` (EH-521) or a `DERIVE … AS name` alias declared earlier.
    pub(super) fn return_stage(&mut self) -> Result<Op, UqlError> {
        let mut channels = vec![self.channel()?];
        while self.eat(&Tok::Comma) {
            channels.push(self.channel()?);
        }
        Ok(Op::Project { channels })
    }

    fn channel(&mut self) -> Result<String, UqlError> {
        let span = self.cur_span();
        let name = self.name("a score channel")?;
        let known = eg_types::wire::OpKind::score_channels();
        if known.contains(&name.as_str()) || self.is_value_channel(&name) {
            return Ok(name);
        }
        Err(UqlError::new(
            UqlCode::UnknownChannel,
            format!("`{name}` is not a score channel in this build"),
            span,
        )
        .expecting(known.iter().map(|c| format!("`{c}`")).collect()))
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
