//! A bounded, rate-limited sample of authorization denials (IDM-03).
//!
//! Metrics count every denial; this keeps a durable SAMPLE of who was denied
//! what, so an operator can see the shape of refused traffic without the
//! denial path ever doing unbounded work. The sampler is pure: the engine
//! feeds it on the hot path (cheap, in memory) and periodically drains it
//! into the identity audit trail in one durable write.

use std::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};

/// Most samples buffered between drains; beyond it new samples are dropped
/// (and counted), never older ones.
pub const MAX_DENIAL_SAMPLES: usize = 256;
/// One sample per (principal, action) per window.
pub const DENIAL_WINDOW_MS: u64 = 60_000;
/// At most this many samples per window across all principals.
pub const MAX_DENIALS_PER_WINDOW: usize = 64;

/// One sampled denial. No secret, token or request body is ever recorded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DenialSample {
    pub at_ms: u64,
    /// The verified principal (or a one-way fingerprint on a replica).
    pub principal: String,
    /// The capability action or graph access that was refused.
    pub action: String,
    /// A short reason code.
    pub reason: String,
}

/// The in-memory sampler.
#[derive(Debug, Clone, Default)]
pub struct DenialSampler {
    pending: VecDeque<DenialSample>,
    last_seen: BTreeMap<(String, String), u64>,
    window_start_ms: u64,
    window_count: usize,
    dropped: u64,
}

impl DenialSampler {
    /// Offer one denial. Returns whether it was kept.
    pub fn offer(&mut self, sample: DenialSample) -> bool {
        self.roll_window(sample.at_ms);
        let key = (sample.principal.clone(), sample.action.clone());
        let recent = self
            .last_seen
            .get(&key)
            .is_some_and(|seen| sample.at_ms < seen.saturating_add(DENIAL_WINDOW_MS));
        if recent || self.window_count >= MAX_DENIALS_PER_WINDOW {
            return false;
        }
        if self.pending.len() >= MAX_DENIAL_SAMPLES {
            self.dropped += 1;
            return false;
        }
        self.window_count += 1;
        self.last_seen.insert(key, sample.at_ms);
        self.pending.push_back(sample);
        true
    }

    /// Take every pending sample (oldest first) and the drop count since the
    /// last drain.
    pub fn drain(&mut self) -> (Vec<DenialSample>, u64) {
        let dropped = std::mem::take(&mut self.dropped);
        (self.pending.drain(..).collect(), dropped)
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.dropped == 0
    }

    fn roll_window(&mut self, now_ms: u64) {
        if now_ms >= self.window_start_ms.saturating_add(DENIAL_WINDOW_MS) {
            self.window_start_ms = now_ms;
            self.window_count = 0;
            let horizon = now_ms.saturating_sub(DENIAL_WINDOW_MS);
            self.last_seen.retain(|_, seen| *seen >= horizon);
        }
    }
}
