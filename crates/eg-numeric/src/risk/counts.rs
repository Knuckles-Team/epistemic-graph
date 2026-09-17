//! A validated `successes` out of `trials` pair, shared by
//! [`super::binomial::BinomialCounts`] (which additionally requires at least
//! one trial: a binomial interval needs a sample to bound) and
//! [`super::pooling::GroupCounts`] (which allows zero trials: an
//! as-yet-unobserved node in a pooling hierarchy). Both wrap this one
//! validated pair and add only the extra constraint their own guarantee
//! needs, instead of each repeating the constructor and accessors.

use crate::detkernel::{validate, StatResult};

/// `successes <= trials`; nothing else constrained here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub(super) struct Counts {
    pub(super) successes: u64,
    pub(super) trials: u64,
}

impl Counts {
    /// Validate `successes <= trials`.
    pub(super) fn checked(successes: u64, trials: u64) -> StatResult<Self> {
        validate::parameter(successes <= trials, "successes", "successes <= trials")?;
        Ok(Self { successes, trials })
    }
}
