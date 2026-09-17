//! A validated `successes` out of `trials` pair, shared by
//! [`super::binomial::BinomialCounts`] (which additionally requires at least
//! one trial: a binomial interval needs a sample to bound) and
//! [`super::pooling::GroupCounts`] (which allows zero trials: an
//! as-yet-unobserved node in a pooling hierarchy). Both `Deref` to this one
//! validated pair, so its accessors are defined once, and each adds only the
//! extra constraint its own guarantee needs on top of [`Counts::checked`].
//!
//! `Counts` itself is `pub` (a public `Deref::Target` cannot name a more
//! restricted type), but the `counts` module is private and its constructor
//! is `pub(super)`, so nothing outside `risk` can name or build one directly
//! -- only reach its two accessors through `BinomialCounts`/`GroupCounts`.

use crate::detkernel::{validate, StatResult};

/// `successes <= trials`; nothing else constrained here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Counts {
    successes: u64,
    trials: u64,
}

impl Counts {
    /// Validate `successes <= trials`.
    pub(super) fn checked(successes: u64, trials: u64) -> StatResult<Self> {
        validate::parameter(successes <= trials, "successes", "successes <= trials")?;
        Ok(Self { successes, trials })
    }

    /// Successes.
    pub fn successes(self) -> u64 {
        self.successes
    }

    /// Trials.
    pub fn trials(self) -> u64 {
        self.trials
    }
}
