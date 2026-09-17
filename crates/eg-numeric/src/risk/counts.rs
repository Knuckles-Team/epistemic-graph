//! A validated `successes` out of `trials` pair, shared by
//! [`super::binomial::BinomialCounts`] (which additionally requires at least
//! one trial: a binomial interval needs a sample to bound) and
//! [`super::pooling::GroupCounts`] (which allows zero trials: an
//! as-yet-unobserved node in a pooling hierarchy). Both `Deref` to this one
//! validated pair, so its accessors are defined once, and each adds only the
//! extra constraint its own guarantee needs on top of [`Counts::checked`].

use crate::detkernel::{validate, StatResult};

/// `successes <= trials`; nothing else constrained here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub(super) struct Counts {
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
