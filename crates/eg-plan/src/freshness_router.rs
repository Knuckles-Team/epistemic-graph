//! EG-UNIFIED-DATA-PLANE-R011 — routing queries by freshness across native, live, and
//! accelerated paths (Decision 4 of `docs/architecture/unified_data_plane_adr.md`: a bounded
//! wait for read-your-writes, never a silent stale result). This is the `.1` typed-model slice:
//! the named route, and a bounded wait that refuses an unbounded (zero-timeout) declaration.
//! Wiring the real router and EXPLAIN output is a later child.

use std::fmt;

/// Which tier a query was answered from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FreshnessRoute {
    Native,
    Live,
    Accelerated,
}

impl FreshnessRoute {
    /// The route's stable label; the exhaustive match proves every named route has one.
    pub fn label(self) -> &'static str {
        match self {
            FreshnessRoute::Native => "native",
            FreshnessRoute::Live => "live",
            FreshnessRoute::Accelerated => "accelerated",
        }
    }
}

/// A read-your-writes wait declares `timeout_ms == 0`: an instant/unbounded wait cannot be "a
/// bounded wait up to a timeout".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnboundedWait;

impl fmt::Display for UnboundedWait {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "a read-your-writes wait must declare a positive bounded timeout, not zero"
        )
    }
}

impl std::error::Error for UnboundedWait {}

/// A bounded wait for the caller's last committed source position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadYourWritesWait {
    pub timeout_ms: u64,
}

impl ReadYourWritesWait {
    /// Refuses a zero (unbounded-in-effect) timeout.
    pub fn new(timeout_ms: u64) -> Result<Self, UnboundedWait> {
        if timeout_ms == 0 {
            return Err(UnboundedWait);
        }
        Ok(Self { timeout_ms })
    }
}

/// How a bounded read-your-writes wait resolved: always one of exactly two typed outcomes,
/// never an implicit third "return stale data" path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitOutcome {
    Satisfied,
    TimedOutTyped,
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-UNIFIED-DATA-PLANE-R011.1
    #[test]
    fn a_positive_timeout_constructs() {
        let wait = ReadYourWritesWait::new(2_000).unwrap();
        assert_eq!(wait.timeout_ms, 2_000);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R011.1
    #[test]
    fn a_zero_timeout_is_refused() {
        assert_eq!(ReadYourWritesWait::new(0).unwrap_err(), UnboundedWait);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R011.1
    #[test]
    fn the_two_wait_outcomes_are_distinct() {
        assert_ne!(WaitOutcome::Satisfied, WaitOutcome::TimedOutTyped);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R011.1
    #[test]
    fn all_three_routes_are_known() {
        assert_eq!(FreshnessRoute::label(FreshnessRoute::Native), "native");
        assert_eq!(FreshnessRoute::label(FreshnessRoute::Live), "live");
        assert_eq!(
            FreshnessRoute::label(FreshnessRoute::Accelerated),
            "accelerated"
        );
    }
}
