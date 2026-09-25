//! Strategy choice for a foreign∩local join, and the AIMD batch sizer (design §4.3–4.4).

use std::time::Duration;

use super::capability::{FullFetch, SourceCapabilities};
use super::stats::SourceStats;
use super::trace::EstimateProvenance;

/// One round trip is costed as this many rows moved.
const REQUEST_COST_ROWS: f64 = 500.0;
/// A batch that returns faster than this doubles the next batch.
const FAST_REQUEST: Duration = Duration::from_millis(250);
/// First batch size of a bind join (grows toward the source's maximum).
const INITIAL_BATCH: usize = 64;
/// Failed key-lookup batches a fragment tolerates before falling back.
pub(crate) const MAX_BATCH_FAILURES: u32 = 3;

/// How a `ForeignScan { join: true }` over a non-empty input is executed.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum JoinStrategy {
    /// Ship the local ids as batched key lookups.
    BindJoin,
    /// Fetch the source and intersect locally.
    FullFetch,
    /// Neither is allowed (a key-only source whose keys cannot be shipped).
    Refuse(String),
}

/// What the planner knows when it chooses.
pub(crate) struct JoinInputs<'a> {
    pub(crate) caps: &'a SourceCapabilities,
    /// Distinct local ids to ship.
    pub(crate) keys: usize,
    /// Whether every key can be expressed in the source's key predicate.
    pub(crate) keys_expressible: bool,
    pub(crate) max_bind_keys: usize,
    pub(crate) stats: Option<SourceStats>,
}

/// Choose, and say which estimate the choice used.
pub(crate) fn choose_join(inputs: &JoinInputs<'_>) -> (JoinStrategy, EstimateProvenance) {
    let provenance = provenance(inputs.stats);
    let key_only = inputs.caps.full_fetch == FullFetch::RequiresKeys;
    let bindable = inputs.caps.max_keys().is_some()
        && inputs.keys_expressible
        && inputs.keys <= inputs.max_bind_keys;
    let strategy = match (bindable, key_only) {
        (true, true) => JoinStrategy::BindJoin,
        (true, false) if bind_is_cheaper(inputs) => JoinStrategy::BindJoin,
        (true, false) | (false, false) => JoinStrategy::FullFetch,
        (false, true) => JoinStrategy::Refuse(refusal(inputs)),
    };
    (strategy, provenance)
}

fn refusal(inputs: &JoinInputs<'_>) -> String {
    if inputs.keys > inputs.max_bind_keys {
        return super::budget::refusal("bind_keys", inputs.max_bind_keys);
    }
    format!(
        "{}: some local ids cannot be expressed as keys of this source",
        super::budget::REQUIRES_KEYS
    )
}

/// `keys + requests × REQUEST_COST_ROWS < estimated full-fetch rows`; an unobserved source
/// is assumed large (a bind join is then preferred), and a source whose key lookups keep
/// failing is fetched whole.
fn bind_is_cheaper(inputs: &JoinInputs<'_>) -> bool {
    let Some(stats) = inputs.stats else {
        return true;
    };
    if stats.key_lookup_failures >= MAX_BATCH_FAILURES {
        return false;
    }
    if stats.full_samples == 0 {
        return true;
    }
    let max_keys = inputs.caps.max_keys().unwrap_or(1) as f64;
    let requests = (inputs.keys as f64 / max_keys).ceil();
    (inputs.keys as f64) + requests * REQUEST_COST_ROWS < stats.ewma_full_rows
}

fn provenance(stats: Option<SourceStats>) -> EstimateProvenance {
    match stats {
        Some(s) if s.full_samples > 0 => EstimateProvenance::Learned {
            samples: s.full_samples,
            rows: s.ewma_full_rows,
        },
        _ => EstimateProvenance::Default,
    }
}

/// Multiplicative batch sizing: double after a fast success (up to the ceiling); after a
/// failure, halve the failed batch and make that the new ceiling, so a size the source
/// refused is never retried. Three consecutive failures give up on key lookups.
#[derive(Debug)]
pub(crate) struct BatchSizer {
    size: usize,
    max: usize,
    consecutive_failures: u32,
}

impl BatchSizer {
    pub(crate) fn new(max: usize) -> Self {
        let max = max.max(1);
        Self {
            size: INITIAL_BATCH.min(max),
            max,
            consecutive_failures: 0,
        }
    }

    pub(crate) fn size(&self) -> usize {
        self.size
    }

    pub(crate) fn success(&mut self, elapsed: Duration) {
        self.consecutive_failures = 0;
        if elapsed < FAST_REQUEST {
            self.size = (self.size * 2).min(self.max);
        }
    }

    /// Record that a batch of `failed` keys failed; `false` once the fragment should give
    /// up on key lookups.
    pub(crate) fn failure(&mut self, failed: usize) -> bool {
        self.consecutive_failures += 1;
        self.max = (failed / 2).max(1);
        self.size = self.max;
        self.consecutive_failures < MAX_BATCH_FAILURES
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation_opt::capability::{KeyLookup, LimitPushdown, Paging};

    fn caps(full_fetch: FullFetch) -> SourceCapabilities {
        SourceCapabilities {
            key_lookup: KeyLookup::Batched { max_keys: 100 },
            limit: LimitPushdown::Unsupported,
            paging: Paging::Single,
            full_fetch,
        }
    }

    fn inputs(
        caps: &SourceCapabilities,
        keys: usize,
        stats: Option<SourceStats>,
    ) -> JoinInputs<'_> {
        JoinInputs {
            caps,
            keys,
            keys_expressible: true,
            max_bind_keys: 1000,
            stats,
        }
    }

    #[test]
    fn learned_small_sources_are_fetched_whole_and_large_ones_bound() {
        let c = caps(FullFetch::Allowed);
        let small = SourceStats {
            ewma_full_rows: 40.0,
            full_samples: 3,
            ..SourceStats::default()
        };
        let large = SourceStats {
            ewma_full_rows: 1_000_000.0,
            full_samples: 3,
            ..SourceStats::default()
        };
        assert_eq!(
            choose_join(&inputs(&c, 10, Some(small))).0,
            JoinStrategy::FullFetch
        );
        let (strategy, provenance) = choose_join(&inputs(&c, 10, Some(large)));
        assert_eq!(strategy, JoinStrategy::BindJoin);
        assert_eq!(
            provenance,
            EstimateProvenance::Learned {
                samples: 3,
                rows: 1_000_000.0
            }
        );
        assert_eq!(
            choose_join(&inputs(&c, 10, None)).1,
            EstimateProvenance::Default
        );
    }

    #[test]
    fn key_only_sources_refuse_what_they_cannot_bind() {
        let c = caps(FullFetch::RequiresKeys);
        assert_eq!(choose_join(&inputs(&c, 10, None)).0, JoinStrategy::BindJoin);
        let over = choose_join(&inputs(&c, 5000, None)).0;
        assert!(
            matches!(over, JoinStrategy::Refuse(ref e) if e.contains("bind_keys")),
            "{over:?}"
        );
    }

    #[test]
    fn the_batch_sizer_grows_on_fast_success_and_caps_after_failure() {
        let mut sizer = BatchSizer::new(1000);
        assert_eq!(sizer.size(), INITIAL_BATCH);
        sizer.success(Duration::from_millis(1));
        assert_eq!(sizer.size(), INITIAL_BATCH * 2);
        sizer.success(Duration::from_secs(1));
        assert_eq!(
            sizer.size(),
            INITIAL_BATCH * 2,
            "a slow batch does not grow"
        );
        assert!(sizer.failure(100));
        assert_eq!(sizer.size(), 50);
        sizer.success(Duration::from_millis(1));
        assert_eq!(sizer.size(), 50, "a refused size is never probed again");
        assert!(sizer.failure(50));
        assert!(sizer.failure(25));
        assert!(!sizer.failure(12), "the third consecutive failure gives up");
    }
}
