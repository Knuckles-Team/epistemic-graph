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
        Err(self.error(UqlCode::TrailingTokens, "expected the end of the expression"))
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
