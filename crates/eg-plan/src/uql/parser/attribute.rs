//! `ATTRIBUTE <agg> OF ( SCORE | name ) ( LINEAR | SHAPLEY [ SAMPLES n SEED s ] | OWEN BY
//! name )` (EH-523) — contribution attribution over the incoming rows.

use eg_types::wire::Op;
#[cfg(feature = "numeric")]
use eg_types::wire::{AttributionInput, AttributionMethod, AttributionValue};

use super::Parser;
use crate::uql::diag::UqlError;
#[cfg(feature = "numeric")]
use crate::uql::lexer::Tok;

/// `SUM` / `MEAN` / `MAX` / `MIN` by keyword.
#[cfg(feature = "numeric")]
const NAMED_VALUES: [(&str, AttributionValue); 4] = [
    ("SUM", AttributionValue::Sum),
    ("MEAN", AttributionValue::Mean),
    ("MAX", AttributionValue::Max),
    ("MIN", AttributionValue::Min),
];

/// `P1` … `P99` (case-insensitive) as a percentile.
#[cfg(feature = "numeric")]
fn percentile(word: &str) -> Option<u8> {
    let digits = word.strip_prefix(['P', 'p'])?;
    let plain = !digits.is_empty() && digits.len() <= 2 && !digits.starts_with('0');
    let p: u8 = digits.parse().ok().filter(|_| plain)?;
    (1..=99).contains(&p).then_some(p)
}

impl<'a> Parser<'a> {
    gated! { "numeric",
        /// `ATTRIBUTE agg OF input method`.
        fn attribute(&mut self) -> Result<Op, UqlError> {
            let value = self.attribution_value()?;
            self.expect_kw("OF")?;
            let input = if self.eat_kw("SCORE") {
                AttributionInput::Score
            } else {
                AttributionInput::Property {
                    name: self.name("a numeric property (or SCORE)")?,
                }
            };
            Ok(Op::Attribute {
                input,
                value,
                method: self.attribution_method()?,
            })
        }
    }

    #[cfg(feature = "numeric")]
    fn attribution_value(&mut self) -> Result<AttributionValue, UqlError> {
        if let Some((_, value)) = NAMED_VALUES.iter().find(|(kw, _)| self.peek_kw(kw)) {
            self.bump();
            return Ok(*value);
        }
        let p = match self.peek_kind() {
            Some(Tok::Ident(word)) => percentile(word),
            _ => None,
        };
        if let Some(p) = p {
            self.bump();
            return Ok(AttributionValue::Percentile { p });
        }
        Err(self
            .err_here("expected the attributed value: SUM, MEAN, MAX, MIN or a percentile P1..P99")
            .expecting(vec![
                "`SUM`".into(),
                "`MEAN`".into(),
                "`MAX`".into(),
                "`MIN`".into(),
                "`P95`".into(),
            ]))
    }

    #[cfg(feature = "numeric")]
    fn attribution_method(&mut self) -> Result<AttributionMethod, UqlError> {
        if self.eat_kw("LINEAR") {
            return Ok(AttributionMethod::Linear);
        }
        if self.eat_kw("OWEN") {
            self.expect_kw("BY")?;
            return Ok(AttributionMethod::Owen {
                by: self.name("the property that groups rows into unions")?,
            });
        }
        self.expect_kw("SHAPLEY")?;
        if !self.eat_kw("SAMPLES") {
            return Ok(AttributionMethod::Shapley);
        }
        let samples = self.parse_number::<u32>("a SAMPLES count")?;
        self.expect_kw("SEED")?;
        Ok(AttributionMethod::ShapleySampled {
            samples,
            seed: self.parse_number::<u64>("a SEED")?,
        })
    }
}

#[cfg(all(test, feature = "numeric"))]
mod tests {
    use super::percentile;

    #[test]
    fn percentile_words_are_p1_through_p99() {
        assert_eq!(percentile("P95"), Some(95));
        assert_eq!(percentile("p5"), Some(5));
        for bad in ["P0", "P100", "P05", "P", "Px", "Q95", "P9x"] {
            assert_eq!(percentile(bad), None, "{bad}");
        }
    }
}
