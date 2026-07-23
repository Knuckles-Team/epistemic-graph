//! Commit policy for the authoritative redb writer.

use std::time::Duration;

const DEFAULT_COMMIT_INTERVAL: Duration = Duration::from_millis(100);
const MAX_COMMIT_INTERVAL_MS: u64 = 60_000;

/// When the authoritative writer commits a non-barrier batch.
///
/// Acknowledged writes always force an immediate durable commit. This policy
/// controls how background/non-acknowledged internal batches are coalesced; there
/// is deliberately no non-durable mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityPolicy {
    /// Commit accumulated work on a bounded cadence.
    Interval(Duration),
    /// Commit after every drained batch.
    Each,
}

impl DurabilityPolicy {
    /// Parse `EPISTEMIC_GRAPH_REDB_COMMIT_POLICY`.
    ///
    /// Accepted values are `each`, `interval`, or a positive millisecond value up
    /// to 60 seconds. Missing/empty configuration uses a 100 ms interval. Invalid,
    /// zero, and non-durable values fail closed.
    pub fn from_env(raw: Option<&str>) -> Result<Self, String> {
        let value = raw.map(str::trim).unwrap_or("");
        if value.is_empty() || value.eq_ignore_ascii_case("interval") {
            return Ok(Self::Interval(DEFAULT_COMMIT_INTERVAL));
        }
        if value.eq_ignore_ascii_case("each") {
            return Ok(Self::Each);
        }
        let milliseconds = value.parse::<u64>().map_err(|_| {
            "EPISTEMIC_GRAPH_REDB_COMMIT_POLICY must be 'each', 'interval', or positive milliseconds"
                .to_string()
        })?;
        if !(1..=MAX_COMMIT_INTERVAL_MS).contains(&milliseconds) {
            return Err(
                "EPISTEMIC_GRAPH_REDB_COMMIT_POLICY milliseconds must be between 1 and 60000"
                    .to_string(),
            );
        }
        Ok(Self::Interval(Duration::from_millis(milliseconds)))
    }

    pub(crate) fn tick(self) -> Duration {
        match self {
            Self::Interval(interval) => interval,
            Self::Each => Duration::from_secs(1),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_durable_bounded_policies() {
        assert_eq!(
            DurabilityPolicy::from_env(None),
            Ok(DurabilityPolicy::Interval(DEFAULT_COMMIT_INTERVAL))
        );
        assert_eq!(
            DurabilityPolicy::from_env(Some("each")),
            Ok(DurabilityPolicy::Each)
        );
        assert_eq!(
            DurabilityPolicy::from_env(Some("25")),
            Ok(DurabilityPolicy::Interval(Duration::from_millis(25)))
        );
        for rejected in ["off", "0", "60001", "unknown"] {
            assert!(DurabilityPolicy::from_env(Some(rejected)).is_err());
        }
    }
}
