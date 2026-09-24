//! Incremental integer kernels: one step per closed bar (EH-414).
//!
//! Inputs and outputs are milli-ticks. Each kernel holds only what its next
//! step needs, so advancing a series by one bar is O(1) (O(period) memory for
//! the simple average's window). Every division rounds half away from zero.

use std::collections::VecDeque;

use super::fixed::{add, div_round, narrow};
use super::{MarketError, MarketResult, INVALID_REQUEST};

/// A period must be at least one bar.
pub fn require_period(period: u32, what: &str) -> MarketResult<i64> {
    if period == 0 {
        return Err(MarketError::new(
            INVALID_REQUEST,
            format!("{what} period must be >= 1"),
        ));
    }
    Ok(i64::from(period))
}

/// True range: the widest of high-low and the gaps from the previous close. The
/// first bar has no previous close and uses high-low.
pub fn true_range(high: i64, low: i64, prev_close: Option<i64>) -> i64 {
    let span = high - low;
    match prev_close {
        None => span,
        Some(close) => span.max((high - close).abs()).max((low - close).abs()),
    }
}

/// Wilder's running average (RMA), seeded by the mean of the first `period`
/// inputs: `next = (prev * (period - 1) + x) / period`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Wilder {
    pub bars: u64,
    pub seed_sum: i64,
    pub value: Option<i64>,
}

impl Wilder {
    pub fn step(&mut self, period: i64, x: i64) -> MarketResult<Option<i64>> {
        self.bars += 1;
        if let Some(prev) = self.value {
            let next = div_round(
                i128::from(prev) * i128::from(period - 1) + i128::from(x),
                i128::from(period),
            );
            self.value = Some(narrow(next, "wilder average")?);
            return Ok(self.value);
        }
        self.seed_sum = add(self.seed_sum, x, "wilder seed")?;
        if self.bars == period as u64 {
            let seed = div_round(i128::from(self.seed_sum), i128::from(period));
            self.value = Some(narrow(seed, "wilder seed")?);
        }
        Ok(self.value)
    }
}

/// Simple moving average over the last `period` inputs.
#[derive(Debug, Clone, Default)]
pub struct Sma {
    window: VecDeque<i64>,
    sum: i128,
}

impl Sma {
    pub fn step(&mut self, period: i64, x: i64) -> MarketResult<Option<i64>> {
        self.window.push_back(x);
        self.sum += i128::from(x);
        if self.window.len() as i64 > period {
            let dropped = self.window.pop_front().unwrap_or_default();
            self.sum -= i128::from(dropped);
        }
        if (self.window.len() as i64) < period {
            return Ok(None);
        }
        narrow(div_round(self.sum, i128::from(period)), "simple average").map(Some)
    }
}

/// Exponential moving average, alpha = 2 / (period + 1), seeded by the simple
/// average of the first `period` inputs.
#[derive(Debug, Clone, Copy, Default)]
pub struct Ema {
    bars: i64,
    seed_sum: i128,
    value: Option<i64>,
}

impl Ema {
    pub fn step(&mut self, period: i64, x: i64) -> MarketResult<Option<i64>> {
        self.bars += 1;
        if let Some(prev) = self.value {
            let delta = div_round(
                (i128::from(x) - i128::from(prev)) * 2,
                i128::from(period + 1),
            );
            self.value = Some(narrow(i128::from(prev) + delta, "exponential average")?);
            return Ok(self.value);
        }
        self.seed_sum += i128::from(x);
        if self.bars == period {
            self.value = Some(narrow(
                div_round(self.seed_sum, i128::from(period)),
                "ema seed",
            )?);
        }
        Ok(self.value)
    }
}

/// One candle in milli-ticks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Candle {
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
}

/// Heikin-Ashi smoothing: close = mean of OHLC, open = mean of the previous
/// Heikin-Ashi open and close (the first: mean of open and close), high/low
/// extended to cover both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HeikinAshi {
    pub open: Option<i64>,
    pub close: Option<i64>,
}

impl HeikinAshi {
    pub fn step(&mut self, raw: Candle) -> MarketResult<Candle> {
        let sum = i128::from(raw.open)
            + i128::from(raw.high)
            + i128::from(raw.low)
            + i128::from(raw.close);
        let close = narrow(div_round(sum, 4), "heikin-ashi close")?;
        let open = match (self.open, self.close) {
            (Some(prev_open), Some(prev_close)) => {
                div_round(i128::from(prev_open) + i128::from(prev_close), 2)
            }
            _ => div_round(i128::from(raw.open) + i128::from(raw.close), 2),
        };
        let open = narrow(open, "heikin-ashi open")?;
        self.open = Some(open);
        self.close = Some(close);
        Ok(Candle {
            open,
            high: raw.high.max(open).max(close),
            low: raw.low.min(open).min(close),
            close,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wilder_seeds_on_the_period_th_input_then_smooths() {
        let mut rma = Wilder::default();
        assert_eq!(rma.step(3, 3_000).unwrap(), None);
        assert_eq!(rma.step(3, 6_000).unwrap(), None);
        assert_eq!(rma.step(3, 9_000).unwrap(), Some(6_000));
        // (6000 * 2 + 3001) / 3 = 5000.33 -> 5000
        assert_eq!(rma.step(3, 3_001).unwrap(), Some(5_000));
    }

    #[test]
    fn simple_and_exponential_averages_match_hand_values() {
        let mut sma = Sma::default();
        let out: Vec<_> = [1_000, 2_000, 3_000, 4_000]
            .iter()
            .map(|&x| sma.step(3, x).unwrap())
            .collect();
        assert_eq!(out, vec![None, None, Some(2_000), Some(3_000)]);
        let mut ema = Ema::default();
        let out: Vec<_> = [1_000, 2_000, 3_000, 7_000]
            .iter()
            .map(|&x| ema.step(3, x).unwrap())
            .collect();
        // seed 2000; then 2000 + (7000-2000)*2/4 = 4500 (the 3000 seeds, not steps)
        assert_eq!(out, vec![None, None, Some(2_000), Some(4_500)]);
    }

    #[test]
    fn heikin_ashi_and_true_range_match_hand_values() {
        let mut ha = HeikinAshi::default();
        let first = ha
            .step(Candle {
                open: 10_000,
                high: 12_000,
                low: 9_000,
                close: 11_000,
            })
            .unwrap();
        assert_eq!(
            first,
            Candle {
                open: 10_500,
                high: 12_000,
                low: 9_000,
                close: 10_500
            }
        );
        let second = ha
            .step(Candle {
                open: 11_000,
                high: 11_500,
                low: 10_800,
                close: 11_200,
            })
            .unwrap();
        assert_eq!(
            second,
            Candle {
                open: 10_500,
                high: 11_500,
                low: 10_500,
                close: 11_125
            }
        );
        assert_eq!(true_range(12, 10, None), 2);
        assert_eq!(true_range(12, 10, Some(15)), 5);
        assert_eq!(true_range(12, 10, Some(7)), 5);
    }

    #[test]
    fn a_zero_period_is_refused() {
        assert_eq!(require_period(0, "atr").unwrap_err().code, INVALID_REQUEST);
    }
}
