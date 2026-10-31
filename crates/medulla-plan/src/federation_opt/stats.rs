//! Learned per-source statistics (design §4.4): what full fetches of a source returned and
//! how long its requests took, keyed by the source's fingerprint — the SHA-256 of its full
//! spec, so identical specs (same credential) share statistics and a different credential is
//! a different source. In memory, bounded, never exported off the process.

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

/// A source's identity for statistics and traces.
pub(crate) type Fingerprint = [u8; 32];

/// EWMA smoothing: recent observations dominate.
const ALPHA: f64 = 0.3;
/// Bound on distinct sources remembered; the least recently observed is evicted.
const MAX_SOURCES: usize = 4096;

/// What past executions observed about one source.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub struct SourceStats {
    /// EWMA of rows a full fetch returned (`0.0` until the first full fetch).
    pub ewma_full_rows: f64,
    /// Full fetches observed.
    pub full_samples: u64,
    /// EWMA of milliseconds per remote request.
    pub ewma_request_ms: f64,
    /// Requests observed.
    pub request_samples: u64,
    /// Key-lookup fragments that failed and fell back to a full fetch.
    pub key_lookup_failures: u32,
    /// Unix milliseconds of the latest observation.
    pub last_observed_unix_ms: u64,
}

impl SourceStats {
    fn ewma(current: f64, samples: u64, value: f64) -> f64 {
        if samples == 0 {
            value
        } else {
            ALPHA * value + (1.0 - ALPHA) * current
        }
    }
}

/// The SHA-256 fingerprint of a label (a spec's canonical bytes, or `named:<name>`).
pub(crate) fn fingerprint(bytes: &[u8]) -> Fingerprint {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes).into()
}

/// The first 8 hex digits of a fingerprint — enough to tell sources apart in a trace.
pub(crate) fn short(fp: &Fingerprint) -> String {
    hex::encode(&fp[..4])
}

fn store() -> &'static RwLock<HashMap<Fingerprint, SourceStats>> {
    static STORE: OnceLock<RwLock<HashMap<Fingerprint, SourceStats>>> = OnceLock::new();
    STORE.get_or_init(|| RwLock::new(HashMap::new()))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// The learned statistics of `fp`, if any execution observed it.
/// Statistics are advisory: a poisoned store (a panic mid-update) is simply not consulted
/// or updated again, which degrades to the default estimates — never a wrong answer.
pub(crate) fn lookup(fp: &Fingerprint) -> Option<SourceStats> {
    store().read().ok()?.get(fp).copied()
}

/// Apply `update` to `fp`'s statistics (creating them, evicting the stalest when full).
fn observe(fp: &Fingerprint, update: impl FnOnce(&mut SourceStats)) {
    let Ok(mut map) = store().write() else {
        return;
    };
    if !map.contains_key(fp) && map.len() >= MAX_SOURCES {
        let stalest = map
            .iter()
            .min_by_key(|(_, s)| s.last_observed_unix_ms)
            .map(|(k, _)| *k);
        if let Some(k) = stalest {
            map.remove(&k);
        }
    }
    let stats = map.entry(*fp).or_default();
    update(stats);
    stats.last_observed_unix_ms = now_ms();
}

/// A full fetch of `fp` returned `rows` rows.
pub(crate) fn observe_full_fetch(fp: &Fingerprint, rows: usize) {
    observe(fp, |s| {
        s.ewma_full_rows = SourceStats::ewma(s.ewma_full_rows, s.full_samples, rows as f64);
        s.full_samples += 1;
    });
}

/// One request to `fp` took `ms` milliseconds.
pub(crate) fn observe_request(fp: &Fingerprint, ms: u64) {
    observe(fp, |s| {
        s.ewma_request_ms = SourceStats::ewma(s.ewma_request_ms, s.request_samples, ms as f64);
        s.request_samples += 1;
    });
}

/// A key-lookup fragment against `fp` failed and fell back.
pub(crate) fn observe_key_failure(fp: &Fingerprint) {
    observe(fp, |s| {
        s.key_lookup_failures = s.key_lookup_failures.saturating_add(1)
    });
}

/// Every source's statistics, labelled by its short fingerprint, sorted — the queryable
/// artifact behind EXPLAIN's "estimate provenance".
pub fn stats_snapshot() -> Vec<(String, SourceStats)> {
    let Ok(map) = store().read() else {
        return Vec::new();
    };
    let mut out: Vec<(String, SourceStats)> = map.iter().map(|(k, v)| (short(k), *v)).collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}
