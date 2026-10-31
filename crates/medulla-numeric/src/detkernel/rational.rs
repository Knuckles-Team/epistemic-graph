//! Exact rational probabilities.
//!
//! Levels (`alpha`, `epsilon`, `delta`) and logging propensities are recorded as
//! reduced `u64/u64` fractions, so ranks such as `ceil((n + 1)(1 - alpha))` are
//! computed in integers and a record states exactly the level it was built at.

use super::error::{StatError, StatResult};

/// Largest accepted denominator.
pub const MAX_DENOMINATOR: u64 = 1_000_000_000_000;

fn gcd(mut a: u128, mut b: u128) -> u128 {
    while b != 0 {
        let r = a % b;
        a = b;
        b = r;
    }
    a
}

/// A reduced fraction `numerator / denominator` in `[0, 1]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UnitRational {
    numerator: u64,
    denominator: u64,
}

impl UnitRational {
    /// Build and reduce `numerator / denominator`; refuses a zero or oversized
    /// denominator and a value above one.
    pub fn new(numerator: u64, denominator: u64) -> StatResult<Self> {
        if denominator == 0 || denominator > MAX_DENOMINATOR {
            return Err(StatError::InvalidParameter {
                name: "denominator",
                requirement: "1 <= denominator <= 1e12",
            });
        }
        if numerator > denominator {
            return Err(StatError::InvalidParameter {
                name: "numerator",
                requirement: "numerator <= denominator",
            });
        }
        let divisor = gcd(numerator as u128, denominator as u128) as u64;
        Ok(Self {
            numerator: numerator / divisor,
            denominator: denominator / divisor,
        })
    }

    /// The reduced numerator.
    pub fn numerator(self) -> u64 {
        self.numerator
    }

    /// The reduced denominator.
    pub fn denominator(self) -> u64 {
        self.denominator
    }

    /// The nearest `f64` (a single correctly rounded division).
    pub fn to_f64(self) -> f64 {
        self.numerator as f64 / self.denominator as f64
    }

    /// `true` for exactly zero.
    pub fn is_zero(self) -> bool {
        self.numerator == 0
    }
}

/// A level strictly inside `(0, 1)`: a miscoverage `alpha`, a risk target
/// `epsilon` or a failure probability `delta`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Level(UnitRational);

impl Level {
    /// `numerator / denominator`, which must lie strictly between 0 and 1.
    pub fn new(numerator: u64, denominator: u64) -> StatResult<Self> {
        let value = UnitRational::new(numerator, denominator)?;
        if value.numerator == 0 || value.numerator == value.denominator {
            return Err(StatError::InvalidParameter {
                name: "level",
                requirement: "0 < level < 1",
            });
        }
        Ok(Self(value))
    }

    /// The exact fraction.
    pub fn rational(self) -> UnitRational {
        self.0
    }

    /// The nearest `f64`.
    pub fn to_f64(self) -> f64 {
        self.0.to_f64()
    }

    /// `ceil((n + 1) * (1 - level))`, the split-conformal order-statistic rank,
    /// computed exactly.
    pub fn conformal_rank(self, n: u64) -> u64 {
        let (num, den) = (self.0.numerator as u128, self.0.denominator as u128);
        let scaled = (n as u128 + 1) * (den - num);
        scaled.div_ceil(den) as u64
    }

    /// The smallest `n` for which [`Level::conformal_rank`] is at most `n`, i.e.
    /// `ceil((1 - level) / level)`: below it every split-conformal set is trivial.
    pub fn conformal_minimum_n(self) -> u64 {
        (self.0.denominator - self.0.numerator).div_ceil(self.0.numerator)
    }
}

/// An exact logging propensity in `[0, 1]`. Zero is representable so that a
/// deterministic logger's unchosen actions can be recorded, and refused later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Propensity(UnitRational);

impl Propensity {
    /// `numerator / denominator` in `[0, 1]`.
    pub fn new(numerator: u64, denominator: u64) -> StatResult<Self> {
        UnitRational::new(numerator, denominator).map(Self)
    }

    /// The exact fraction.
    pub fn rational(self) -> UnitRational {
        self.0
    }

    /// The nearest `f64`.
    pub fn to_f64(self) -> f64 {
        self.0.to_f64()
    }

    /// `true` for exactly zero.
    pub fn is_zero(self) -> bool {
        self.0.is_zero()
    }

    /// `target / self` as an importance weight; `None` for a zero propensity.
    pub fn importance_weight(self, target: f64) -> Option<f64> {
        if self.is_zero() {
            return None;
        }
        Some(target * self.0.denominator as f64 / self.0.numerator as f64)
    }
}

/// `true` when the propensities sum to exactly one.
pub fn sums_to_one(propensities: &[Propensity]) -> StatResult<bool> {
    let mut numerator: u128 = 0;
    let mut denominator: u128 = 1;
    for p in propensities {
        let (n, d) = (p.0.numerator as u128, p.0.denominator as u128);
        let overflow = StatError::ArithmeticOverflow {
            what: "propensity sum",
        };
        numerator = numerator
            .checked_mul(d)
            .and_then(|left| n.checked_mul(denominator).and_then(|r| left.checked_add(r)))
            .ok_or(overflow)?;
        denominator = denominator.checked_mul(d).ok_or(overflow)?;
        let divisor = gcd(numerator, denominator);
        numerator /= divisor;
        denominator /= divisor;
    }
    Ok(numerator == denominator)
}
