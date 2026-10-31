//! The sign-in throttle: per-account and per-network exponential backoff
//! (§5.1). A soft lock only: the account is never hard-locked, so an
//! attacker cannot lock an administrator out for good; the break-glass
//! administrator is exempt from the per-network throttle.

use serde::{Deserialize, Serialize};

use super::IdentityStore;
use crate::identity::MAX_THROTTLE_KEYS;

/// Failures tolerated inside one window before backoff starts.
pub const FREE_FAILURES: u32 = 5;
/// The failure-counting window.
pub const THROTTLE_WINDOW_MS: u64 = 15 * 60 * 1000;
/// Longest backoff.
pub const MAX_BACKOFF_MS: u64 = 15 * 60 * 1000;

/// One throttle key's state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThrottleEntry {
    pub failures: u32,
    pub window_start_ms: u64,
    pub next_allowed_at_ms: u64,
}

/// The backoff after `failures` failures in one window.
fn backoff_ms(failures: u32) -> u64 {
    if failures < FREE_FAILURES {
        return 0;
    }
    let exponent = (failures - FREE_FAILURES).min(20);
    (1_000u64 << exponent).min(MAX_BACKOFF_MS)
}

pub(crate) fn account_key(principal_id: &str) -> String {
    format!("acct:{principal_id}")
}

pub(crate) fn network_key(ip_prefix: &str) -> String {
    format!("ip:{ip_prefix}")
}

impl IdentityStore {
    /// When `key` may try again (`None`: now).
    pub(crate) fn throttled_until(&self, key: &str, now_ms: u64) -> Option<u64> {
        self.throttle
            .get(key)
            .map(|entry| entry.next_allowed_at_ms)
            .filter(|until| *until > now_ms)
    }

    /// Count one failure against `key`.
    pub(crate) fn record_failure(&mut self, key: String, now_ms: u64) {
        let entry = self.throttle.entry(key).or_insert(ThrottleEntry {
            failures: 0,
            window_start_ms: now_ms,
            next_allowed_at_ms: 0,
        });
        if now_ms >= entry.window_start_ms.saturating_add(THROTTLE_WINDOW_MS) {
            entry.failures = 0;
            entry.window_start_ms = now_ms;
        }
        entry.failures = entry.failures.saturating_add(1);
        entry.next_allowed_at_ms = now_ms.saturating_add(backoff_ms(entry.failures));
        self.evict_throttle(now_ms);
    }

    /// Forget `key` (a success, or an administrator's unlock).
    pub(crate) fn clear_throttle(&mut self, key: &str) {
        self.throttle.remove(key);
    }

    /// Keep the table bounded: expired windows first, then the oldest.
    fn evict_throttle(&mut self, now_ms: u64) {
        if self.throttle.len() <= MAX_THROTTLE_KEYS {
            return;
        }
        self.throttle.retain(|_, entry| {
            entry.next_allowed_at_ms > now_ms
                || now_ms < entry.window_start_ms.saturating_add(THROTTLE_WINDOW_MS)
        });
        while self.throttle.len() > MAX_THROTTLE_KEYS {
            let oldest = self
                .throttle
                .iter()
                .min_by_key(|(_, entry)| entry.window_start_ms)
                .map(|(key, _)| key.clone());
            match oldest {
                Some(key) => self.throttle.remove(&key),
                None => break,
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::backoff_ms;

    #[test]
    fn backoff_starts_after_the_free_failures_and_is_capped() {
        assert_eq!(backoff_ms(4), 0);
        assert_eq!(backoff_ms(5), 1_000);
        assert_eq!(backoff_ms(6), 2_000);
        assert_eq!(backoff_ms(40), super::MAX_BACKOFF_MS);
    }
}
