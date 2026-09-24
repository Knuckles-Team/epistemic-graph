//! One RAII timer for the `commit_ops` durable-write phases (EH-290,
//! CONCEPT:EG-KG.storage.commit-ops-phase-timing).
//!
//! The raft append/commit path measured `commit_ops` at a 1.31s median commit
//! against a 6.5ms median queue-wait -- the write itself is slow, not
//! contention (`plans/refactor/architecture/STORAGE-DURABILITY-PERFORMANCE-DESIGN.md`).
//! This is the ONE owner for "measure and record a named commit phase", used
//! from every phase call site in `store_control::commit_ops`/`commit_drained_chunk`
//! and `shard::Shard::commit_drain`, so adding a phase is one
//! `CommitPhaseTimer::start(..)` / `.finish()` pair rather than a repeated
//! `Instant::now()`/`elapsed()` block that `dupehound`/`jscpd` would flag as a
//! structural clone across call sites.
//!
//! Each timer also opens one `commit_phase` tracing span carrying its `phase`
//! label, so a subscriber (the `redb_write_latency_bench` phase collector, or
//! any DEBUG trace) sees the same decomposition per sample -- percentiles, not
//! only the histogram's mean. The storage kernel's own phases
//! (`redb_begin_write`, `write_authority_validation`, `redb_commit`) emit the
//! same span shape from `eg-storage`, which has no metrics registry.

use std::time::Instant;

/// A started phase of one `commit_ops` drain.
///
/// Dropping it without calling [`Self::finish`] (an early `?` return partway
/// through a phase, for instance) loses the sample silently rather than
/// panicking or corrupting the commit: this timer is diagnostic only, never
/// load-bearing.
pub(crate) struct CommitPhaseTimer {
    started: Instant,
    phase: &'static str,
    /// Closed when the timer is consumed or dropped; never entered, because a
    /// phase is a wall-clock interval on the writer thread, not a scope.
    _span: tracing::Span,
}

impl CommitPhaseTimer {
    /// Start timing `phase`. The label set is fixed and small -- see
    /// `crate::metrics::observe_commit_ops_phase`'s doc for the exact values --
    /// so cardinality never grows with graphs, drains or callers.
    pub(crate) fn start(phase: &'static str) -> Self {
        Self {
            started: Instant::now(),
            phase,
            _span: tracing::debug_span!("commit_phase", phase),
        }
    }

    /// Stop timing and record the elapsed wall time against this phase's
    /// histogram series.
    pub(crate) fn finish(self) {
        crate::metrics::observe_commit_ops_phase(self.phase, self.started.elapsed().as_secs_f64());
    }
}
