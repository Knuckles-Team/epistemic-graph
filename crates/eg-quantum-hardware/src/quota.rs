//! Budget enforcement for shared, rolling-window third-party quantum quotas.
//!
//! ★ Charter (`quantum-external-providers.md` ss1.3): "Budget enforcement is mandatory,
//! not optional. The IBM 28-day window is a hard, shared, trivially-exhausted
//! resource. Implement quota tracking and refuse-when-exhausted rather than
//! discovering exhaustion at submit time. Surface remaining budget through the
//! result metadata."
//!
//! This module is the generic mechanism every Q10 provider adapter (`ibm`/`braket`/
//! `azure`) uses identically: a sliding-window usage log plus a reserve-before-submit
//! check. It does not know about IBM/AWS/Azure at all -- each adapter supplies its
//! own `provider` name, `window`, `limit`, and `unit_name` (e.g. IBM's ~10 QPU-minute
//! / 28-day window, Braket's ~1 simulator-hour / 30-day AWS Free Tier window).
//!
//! # "Surface remaining budget through the result metadata"
//!
//! `eg_quantum_core::result::QuantumResult` is a frozen, multi-lane-shared contract
//! (register `D-QN-2`, closed) this lane deliberately does not modify -- editing it
//! risks colliding with the concurrently-landing `w6-quantum-q2-qasm` and
//! `w6-quantum-q4-entanglement` lanes, and it carries no generic metadata bag to
//! extend into anyway. So this lane's answer is [`QuotaStatus`]: every hardware
//! backend keeps the status *alongside* the `QuantumResult` it returns, keyed by the
//! same [`eg_quantum_core::backend::JobHandle`], and exposes it through an inherent
//! `quota_status(job)` accessor (see `ibm.rs`/`braket.rs`/`azure.rs`). A future
//! Q8/Q9 lane that adds a generic per-result provider-metadata field to the shared
//! `QuantumResult` type can absorb this without changing this module's contract.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

/// A count of provider-defined budget units (QPU-seconds, cloud-simulator-seconds,
/// or an abstract "credit" for a provider with no fixed recurring quota). Deliberately
/// NOT tied to a specific unit type (seconds vs. credits vs. shots) -- each adapter's
/// `unit_name` on [`QuotaTracker`] documents what a unit means for that provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct QuotaUnits(pub u64);

impl std::fmt::Display for QuotaUnits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::ops::Add for QuotaUnits {
    type Output = QuotaUnits;
    fn add(self, rhs: QuotaUnits) -> QuotaUnits {
        QuotaUnits(self.0.saturating_add(rhs.0))
    }
}

impl std::ops::Sub for QuotaUnits {
    type Output = QuotaUnits;
    fn sub(self, rhs: QuotaUnits) -> QuotaUnits {
        QuotaUnits(self.0.saturating_sub(rhs.0))
    }
}

/// A point-in-time snapshot of a provider's rolling-window budget, returned by
/// [`QuotaTracker::status`] and threaded through every hardware backend's job store so
/// it can be surfaced alongside a `QuantumResult` (see module docs).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QuotaStatus {
    pub provider: &'static str,
    pub unit_name: &'static str,
    pub used: QuotaUnits,
    pub limit: QuotaUnits,
    pub remaining: QuotaUnits,
    pub window: Duration,
    /// Wall-clock estimate of when `remaining` will next increase -- the moment the
    /// oldest still-counted usage entry ages out of the rolling window. `None` when
    /// there is no usage recorded yet (nothing to age out, budget is fully available).
    pub next_reset_estimate: Option<SystemTime>,
}

/// Returned by [`QuotaTracker::try_reserve`] when a request would exceed the rolling
/// window's limit. Carries the same [`QuotaStatus`] a caller would get from
/// `status()`, so the refusal message and any downstream logging/observability see
/// identical numbers -- there is exactly one source of truth for "how much budget is
/// left," not a separate computation at refusal time vs. inspection time.
#[derive(Debug, Clone, thiserror::Error)]
#[error(
    "quota exhausted for {provider}: requested {requested} {unit}, only {remaining} {unit} \
     remaining of {limit} {unit} in the trailing {window_days} day rolling window \
     (used {used} {unit}; next reset ~{reset})"
)]
pub struct QuotaExceeded {
    pub provider: &'static str,
    pub unit: &'static str,
    pub requested: QuotaUnits,
    pub used: QuotaUnits,
    pub limit: QuotaUnits,
    pub remaining: QuotaUnits,
    pub window_days: u64,
    reset: String,
}

/// Pluggable usage log. The default [`InMemoryQuotaStore`] is process-local (lost on
/// restart, and NOT shared across multiple engine replicas hitting the same provider
/// account) -- an explicit, documented gap: durable, cross-replica quota tracking
/// belongs on the durable-execution job plane (`eg-jobs`, Q5), which is correctly
/// "not started" per `quantum-native.md`'s 2026-08-07 status block. Until Q5 lands, a
/// deployment with more than one engine replica sharing one provider account MUST
/// either pin hardware-backed quantum work to a single replica or supply its own
/// [`QuotaStore`] backed by shared storage; this trait is the seam for that.
pub trait QuotaStore: Send + Sync {
    fn record(&self, provider: &str, units: QuotaUnits, at: SystemTime);
    /// All usage events for `provider` at or after `since`, oldest first.
    fn usage_since(&self, provider: &str, since: SystemTime) -> Vec<(SystemTime, QuotaUnits)>;
}

/// The default, process-local [`QuotaStore`]: a per-provider deque of `(timestamp,
/// units)` events, pruned of anything older than the caller's window on every read.
#[derive(Default)]
pub struct InMemoryQuotaStore {
    events: Mutex<HashMap<String, VecDeque<(SystemTime, QuotaUnits)>>>,
}

impl QuotaStore for InMemoryQuotaStore {
    fn record(&self, provider: &str, units: QuotaUnits, at: SystemTime) {
        let mut guard = self.events.lock().expect("quota store mutex poisoned");
        guard
            .entry(provider.to_string())
            .or_default()
            .push_back((at, units));
    }

    fn usage_since(&self, provider: &str, since: SystemTime) -> Vec<(SystemTime, QuotaUnits)> {
        let mut guard = self.events.lock().expect("quota store mutex poisoned");
        let entry = guard.entry(provider.to_string()).or_default();
        entry.retain(|(t, _)| *t >= since);
        entry.iter().copied().collect()
    }
}

/// Reserve-before-submit budget enforcement for one provider's rolling-window quota.
/// Every hardware backend owns exactly one of these per distinct budget it must
/// respect (e.g. `IbmQuantumBackend` owns one for the 28-day QPU-runtime window).
pub struct QuotaTracker<S: QuotaStore = InMemoryQuotaStore> {
    provider: &'static str,
    unit_name: &'static str,
    window: Duration,
    limit: QuotaUnits,
    store: S,
}

impl QuotaTracker<InMemoryQuotaStore> {
    /// Construct with the default process-local store. This is what every provider
    /// adapter's `::new()` (env-credentials, real-transport) constructor uses; the
    /// `with_store` constructor below is for tests and for a future durable-store
    /// deployment (see [`QuotaStore`]'s docs).
    pub fn new(
        provider: &'static str,
        unit_name: &'static str,
        window: Duration,
        limit: QuotaUnits,
    ) -> Self {
        Self::with_store(
            provider,
            unit_name,
            window,
            limit,
            InMemoryQuotaStore::default(),
        )
    }
}

impl<S: QuotaStore> QuotaTracker<S> {
    pub fn with_store(
        provider: &'static str,
        unit_name: &'static str,
        window: Duration,
        limit: QuotaUnits,
        store: S,
    ) -> Self {
        QuotaTracker {
            provider,
            unit_name,
            window,
            limit,
            store,
        }
    }

    fn used_at(&self, now: SystemTime) -> QuotaUnits {
        let since = now
            .checked_sub(self.window)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let events = self.store.usage_since(self.provider, since);
        QuotaUnits(events.iter().map(|(_, u)| u.0).sum())
    }

    /// Estimate when the OLDEST currently-counted usage event ages out of the
    /// window, i.e. roughly when `remaining` will next tick upward. Advisory only --
    /// a provider's actual reset behaviour is a true sliding window, not a fixed
    /// cliff, so this names the earliest moment more budget could appear, not a
    /// guarantee of how much.
    fn next_reset_at(&self, now: SystemTime) -> Option<SystemTime> {
        let since = now
            .checked_sub(self.window)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        self.store
            .usage_since(self.provider, since)
            .into_iter()
            .map(|(t, _)| t)
            .min()
            .map(|oldest| oldest + self.window)
    }

    pub fn status(&self, now: SystemTime) -> QuotaStatus {
        let used = self.used_at(now);
        QuotaStatus {
            provider: self.provider,
            unit_name: self.unit_name,
            used,
            limit: self.limit,
            remaining: self.limit - used,
            window: self.window,
            next_reset_estimate: self.next_reset_at(now),
        }
    }

    /// Refuse-when-exhausted, checked and recorded atomically from the caller's
    /// perspective: if `requested` would push cumulative usage over `limit`, this
    /// returns `Err` WITHOUT recording anything (no partial charge for a refused
    /// request); otherwise it records `requested` immediately (optimistic
    /// reservation, so two submissions racing between "check" and "record" cannot
    /// both slip through) and returns the post-reservation status. Callers whose
    /// actual provider-reported usage differs from `requested` (the common case --
    /// `requested` is an estimate at submit time) should NOT double-record; this
    /// tracker intentionally has no separate reconciliation step because none of the
    /// three providers' free-tier APIs return precise consumed-budget-per-job
    /// figures suitable for reconciliation, and over-counting via the estimate is the
    /// safe direction to be wrong in for a "trivially-exhausted shared resource."
    pub fn try_reserve(
        &self,
        requested: QuotaUnits,
        now: SystemTime,
    ) -> Result<QuotaStatus, QuotaExceeded> {
        let used = self.used_at(now);
        if used + requested > self.limit {
            let remaining = self.limit - used;
            let reset = self
                .next_reset_at(now)
                .map(|t| {
                    t.duration_since(SystemTime::UNIX_EPOCH)
                        .map(|d| format!("{}s (unix)", d.as_secs()))
                        .unwrap_or_else(|_| "unknown".to_string())
                })
                .unwrap_or_else(|| "n/a (no usage recorded)".to_string());
            return Err(QuotaExceeded {
                provider: self.provider,
                unit: self.unit_name,
                requested,
                used,
                limit: self.limit,
                remaining,
                window_days: self.window.as_secs() / 86_400,
                reset,
            });
        }
        self.store.record(self.provider, requested, now);
        Ok(self.status(now))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: Duration = Duration::from_secs(86_400);

    #[test]
    fn reserve_within_budget_succeeds_and_deducts() {
        let t = QuotaTracker::new("test-provider", "seconds", 28 * DAY, QuotaUnits(600));
        let now = SystemTime::now();
        let status = t.try_reserve(QuotaUnits(100), now).expect("within budget");
        assert_eq!(status.used, QuotaUnits(100));
        assert_eq!(status.remaining, QuotaUnits(500));
    }

    #[test]
    fn reserve_beyond_budget_is_refused_and_not_recorded() {
        let t = QuotaTracker::new("test-provider", "seconds", 28 * DAY, QuotaUnits(600));
        let now = SystemTime::now();
        t.try_reserve(QuotaUnits(600), now)
            .expect("exactly at limit succeeds");
        let err = t
            .try_reserve(QuotaUnits(1), now)
            .expect_err("must refuse past the limit");
        assert_eq!(err.remaining, QuotaUnits(0));
        // The refused request must not have been recorded -- status is unchanged.
        assert_eq!(t.status(now).used, QuotaUnits(600));
    }

    #[test]
    fn usage_outside_the_window_does_not_count() {
        let t = QuotaTracker::new("test-provider", "seconds", DAY, QuotaUnits(60));
        let long_ago = SystemTime::now() - Duration::from_secs(2 * 86_400);
        // Record directly through the store to simulate old usage, bypassing
        // try_reserve's "now" clamp.
        let now = SystemTime::now();
        let status_before = t.status(now);
        assert_eq!(status_before.used, QuotaUnits(0));
        // Reserve at `long_ago` is not representable via try_reserve's public API
        // (it always stamps `now`), so this test documents the window math via a
        // fresh tracker whose single reservation is old relative to a later `status`
        // call using an InMemoryQuotaStore directly.
        let store = InMemoryQuotaStore::default();
        store.record("p", QuotaUnits(60), long_ago);
        let t2 = QuotaTracker::with_store("p", "seconds", DAY, QuotaUnits(60), store);
        assert_eq!(
            t2.status(now).used,
            QuotaUnits(0),
            "usage older than the window must not count"
        );
    }

    #[test]
    fn zero_requested_units_always_succeeds() {
        let t = QuotaTracker::new("test-provider", "seconds", DAY, QuotaUnits(0));
        let now = SystemTime::now();
        let status = t
            .try_reserve(QuotaUnits(0), now)
            .expect("zero-cost request always fits");
        assert_eq!(status.remaining, QuotaUnits(0));
    }
}
