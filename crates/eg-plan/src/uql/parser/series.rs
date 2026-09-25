//! `DERIVE` and the series-expression sub-grammar (EH-522): function calls over value
//! channels and numbers, `func(args…, params…)`, resolved against the fixed function table
//! `eg_types::series_expr::SIGNATURES`. The printer is `eg_types::wire::uql_series_expr`.

use eg_types::series_expr::{SeriesExpr, SeriesFunc};

use super::Parser;
use crate::uql::diag::{UqlCode, UqlError};
use crate::uql::lexer::Tok;

/// What a `DERIVE` column may read besides `v0..vk` and declared aliases.
const SCORE_CHANNEL: &str = "score";

impl Parser<'_> {
    gated! { "timeseries",
        /// `DERIVE sexpr AS name {, sexpr AS name}`.
        fn derive(&mut self) -> Result<eg_types::wire::Op, UqlError> {
            let mut columns = vec![self.derive_column()?];
            while self.eat(&Tok::Comma) {
                columns.push(self.derive_column()?);
            }
            Ok(eg_types::wire::Op::Derive { columns })
        }
    }

    gated! { "timeseries",
        /// `SKILL name AGAINST name HORIZONS [int, …] WINDOW int [BOOTSTRAP int SEED int]`.
        fn skill(&mut self) -> Result<eg_types::wire::Op, UqlError> {
            let feature = self.skill_channel()?;
            self.expect_kw("AGAINST")?;
            let outcome = self.skill_channel()?;
            self.expect_kw("HORIZONS")?;
            let span = self.cur_span();
            let horizons = self.bracket_list("the horizons", |p| p.parse_number::<u64>("a horizon"))?;
            self.expect_kw("WINDOW")?;
            let window = self.parse_number::<u64>("the IC window")?;
            if horizons.is_empty() || window < 2 {
                return Err(UqlError::new(
                    UqlCode::InvalidRange,
                    "SKILL needs at least one horizon and an IC window of at least 2",
                    span,
                ));
            }
            let (resamples, seed) = self.skill_bootstrap()?;
            self.derived
                .extend(eg_types::series_expr::SKILL_CHANNELS.iter().map(|c| c.to_string()));
            Ok(eg_types::wire::Op::Skill {
                spec: eg_types::series_expr::SkillOp { feature, outcome, horizons, window, resamples, seed },
            })
        }
    }

    #[cfg(feature = "timeseries")]
    fn skill_channel(&mut self) -> Result<String, UqlError> {
        let span = self.cur_span();
        let name = self.name("a value channel")?;
        match self.channel_ref(name, span)? {
            SeriesExpr::Channel { name } => Ok(name),
            SeriesExpr::Const { .. } | SeriesExpr::Call { .. } => {
                Err(self.err_here("expected a value channel"))
            }
        }
    }

    #[cfg(feature = "timeseries")]
    fn skill_bootstrap(&mut self) -> Result<(u64, u64), UqlError> {
        if !self.eat_kw("BOOTSTRAP") {
            return Ok((0, 0));
        }
        let resamples = self.parse_number::<u64>("the bootstrap resamples")?;
        self.expect_kw("SEED")?;
        Ok((resamples, self.parse_number::<u64>("a seed")?))
    }

    gated! { "timeseries",
        /// `MOTIF OF c (LIKE [num, …] | LENGTH int) TOP int [SEED int]` (EH-529): query-
        /// by-example (MASS) with `LIKE`, or the matrix-profile motifs with `LENGTH`.
        fn motif(&mut self) -> Result<eg_types::wire::Op, UqlError> {
            self.expect_kw("OF")?;
            let channel = self.skill_channel()?;
            let span = self.cur_span();
            let search = if self.eat_kw("LIKE") {
                let shape = self.bracket_list("the shape", |p| p.number("a shape value"))?;
                self.check_motif_length(shape.len(), span)?;
                eg_types::series_expr::MotifSearch::Like { shape }
            } else {
                self.expect_kw("LENGTH")?;
                let length = self.motif_length(span)?;
                eg_types::series_expr::MotifSearch::Pairs { length }
            };
            self.motif_op(channel, search)
        }
    }

    gated! { "timeseries",
        /// `DISCORD OF c LENGTH int TOP int [SEED int]` (EH-529): the matrix-profile
        /// discords — the subsequences farthest from their nearest neighbour.
        fn discord(&mut self) -> Result<eg_types::wire::Op, UqlError> {
            self.expect_kw("OF")?;
            let channel = self.skill_channel()?;
            let span = self.cur_span();
            self.expect_kw("LENGTH")?;
            let length = self.motif_length(span)?;
            self.motif_op(channel, eg_types::series_expr::MotifSearch::Discord { length })
        }
    }

    gated! { "timeseries",
        /// `EVENTS c {, c}` (EH-529): every series row whose value channel `c` is
        /// non-zero becomes an event row a later `CEP` stage matches by key `c`. Names
        /// a channel by its bare identifier, NOT validated against `v0..vk` / earlier
        /// `DERIVE` aliases like [`Self::skill_channel`] does — a channel a native
        /// source (not a `DERIVE`) already carries is just as legal a key, and a name
        /// this build never sees a value for simply emits no event (never a parse-time
        /// refusal for a plan whose earlier stages this parser cannot see, e.g. inside
        /// a `LET` binding used before its own definition is walked).
        fn events(&mut self) -> Result<eg_types::wire::Op, UqlError> {
            let mut channels = vec![self.name("a value channel")?];
            while self.eat(&Tok::Comma) {
                channels.push(self.name("a value channel")?);
            }
            Ok(eg_types::wire::Op::Events { channels })
        }
    }

    /// `TOP int [SEED int]`, then the `MotifOp`. Shared tail of `motif` and `discord`.
    #[cfg(feature = "timeseries")]
    fn motif_op(
        &mut self,
        channel: String,
        search: eg_types::series_expr::MotifSearch,
    ) -> Result<eg_types::wire::Op, UqlError> {
        self.expect_kw("TOP")?;
        let span = self.cur_span();
        let top = self.parse_number::<u64>("TOP k")?;
        if top == 0 {
            return Err(UqlError::new(
                UqlCode::InvalidRange,
                "TOP needs at least 1",
                span,
            ));
        }
        let seed = if self.eat_kw("SEED") {
            self.parse_number::<u64>("a seed")?
        } else {
            0
        };
        self.derived.extend(
            eg_types::series_expr::MOTIF_CHANNELS
                .iter()
                .map(|c| c.to_string()),
        );
        Ok(eg_types::wire::Op::Motif {
            spec: eg_types::series_expr::MotifOp {
                channel,
                search,
                top,
                seed,
            },
        })
    }

    /// A `LENGTH`/`LIKE` subsequence length: at least 2 (`eg_numeric::series::distance::
    /// MIN_LENGTH`, checked again authoritatively where the kernel runs — this parse-time
    /// check only gives an earlier, better-located error).
    #[cfg(feature = "timeseries")]
    fn motif_length(&mut self, span: (usize, usize)) -> Result<u64, UqlError> {
        let length = self.parse_number::<u64>("a subsequence length")?;
        self.check_motif_length(length as usize, span)?;
        Ok(length)
    }

    #[cfg(feature = "timeseries")]
    fn check_motif_length(&self, length: usize, span: (usize, usize)) -> Result<(), UqlError> {
        if length >= 2 {
            return Ok(());
        }
        Err(UqlError::new(
            UqlCode::InvalidRange,
            "a MOTIF/DISCORD subsequence needs a length of at least 2",
            span,
        ))
    }

    #[cfg(feature = "timeseries")]
    fn derive_column(&mut self) -> Result<eg_types::series_expr::DeriveColumn, UqlError> {
        let expr = self.series_expr()?;
        self.expect_kw("AS")?;
        let name = self.name("the derived channel's name")?;
        self.derived.insert(name.clone());
        Ok(eg_types::series_expr::DeriveColumn { expr, name })
    }

    /// One series expression: a number, a channel, or `func(…)`. In a `DERIVE` stage a
    /// channel must be `v0..vk`, `score` or an earlier alias; in a derived-series
    /// definition ([`Parser::free_channels`]) its declared sources bind the names.
    pub(super) fn series_expr(&mut self) -> Result<SeriesExpr, UqlError> {
        if self.at_series_number() {
            let value = self.number("a number")?;
            return Ok(SeriesExpr::Const { value });
        }
        let span = self.cur_span();
        let name = self.name("a series expression (a channel, a number or a function call)")?;
        if !self.peek(&Tok::LParen) {
            return self.channel_ref(name, span);
        }
        let func = SeriesFunc::from_name(&name).ok_or_else(|| unknown_function(&name, span))?;
        self.enter()?;
        self.bump();
        let items = self.call_items()?;
        self.expect(&Tok::RParen, "`)` closing the call")?;
        self.leave();
        let expr = split_call(func, items)
            .map_err(|msg| UqlError::new(UqlCode::UnexpectedToken, msg, span))?;
        expr.check()
            .map_err(|msg| UqlError::new(UqlCode::ExpectedInteger, msg, span))?;
        Ok(expr)
    }

    /// A whole derived-series definition: one expression over free channel names, and
    /// nothing after it.
    pub(in crate::uql) fn series_definition(&mut self) -> Result<SeriesExpr, UqlError> {
        self.free_channels = true;
        let expr = self.series_expr()?;
        if self.at_end() {
            return Ok(expr);
        }
        Err(self.error(
            UqlCode::TrailingTokens,
            "expected the end of the expression",
        ))
    }

    fn at_series_number(&self) -> bool {
        matches!(
            self.peek_kind(),
            Some(Tok::Num(_) | Tok::Param(_) | Tok::Dash)
        )
    }

    fn call_items(&mut self) -> Result<Vec<SeriesExpr>, UqlError> {
        let mut items = Vec::new();
        if self.peek(&Tok::RParen) {
            return Ok(items);
        }
        items.push(self.series_expr()?);
        while self.eat(&Tok::Comma) {
            items.push(self.series_expr()?);
        }
        Ok(items)
    }

    fn channel_ref(&self, name: String, span: (usize, usize)) -> Result<SeriesExpr, UqlError> {
        let known = self.free_channels || name == SCORE_CHANNEL || self.is_value_channel(&name);
        if known {
            return Ok(SeriesExpr::Channel { name });
        }
        Err(UqlError::new(
            UqlCode::UnknownChannel,
            format!("`{name}` is not a value channel here"),
            span,
        )
        .expecting(vec![
            "`v0`…`vN`".into(),
            "`score`".into(),
            "an earlier DERIVE alias".into(),
        ]))
    }
}

fn unknown_function(name: &str, span: (usize, usize)) -> UqlError {
    let names = eg_types::series_expr::SIGNATURES
        .iter()
        .map(|s| format!("`{}`", s.name))
        .collect();
    UqlError::new(
        UqlCode::UnknownFunction,
        format!("`{name}` is not a series function"),
        span,
    )
    .expecting(names)
}

/// Split a call's items into its series arguments and its numeric parameters, by the
/// function's signature. A number may stand as a (constant) series argument; a
/// parameter must be a number.
fn split_call(func: SeriesFunc, mut items: Vec<SeriesExpr>) -> Result<SeriesExpr, String> {
    let sig = func.signature();
    if items.len() != sig.series + sig.params {
        return Err(format!(
            "{}() takes {} series and {} number(s), got {} argument(s)",
            sig.name,
            sig.series,
            sig.params,
            items.len()
        ));
    }
    let params = items
        .split_off(sig.series)
        .into_iter()
        .map(|item| match item {
            SeriesExpr::Const { value } => Ok(value),
            SeriesExpr::Channel { .. } | SeriesExpr::Call { .. } => {
                Err(format!("{}() parameters must be numbers", sig.name))
            }
        })
        .collect::<Result<Vec<f64>, String>>()?;
    Ok(SeriesExpr::call(func, items, params))
}
