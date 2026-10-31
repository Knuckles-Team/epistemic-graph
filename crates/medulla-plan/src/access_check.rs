//! Read-time access checks for sources that do not sync their ACLs
//! (MEDULLA-FEDERATED-QUERY-R703.1, R703.2).
//!
//! A source pack declares its [`AccessMode`]. Hits from a `checked_at_read`
//! source are grouped by source and checked through the source's port in
//! bounded batches before a bundle is compiled. Answers are cached per
//! principal for at most [`MAX_CACHE_TTL_MS`]; a check slower than
//! [`CHECK_TIMEOUT_MS`] drops every hit of that source.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Records sent in one check.
pub const MAX_RECORDS_PER_CHECK: usize = 200;
/// Longest a cached answer is reused.
pub const MAX_CACHE_TTL_MS: u64 = 300_000;
/// A check slower than this drops the source's hits.
pub const CHECK_TIMEOUT_MS: u64 = 2_000;

/// How a source's permissions reach the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessMode {
    /// ACLs are synced with the records; no read-time check.
    Synced,
    /// Every hit is checked with the source at read time.
    CheckedAtRead,
}

/// One bounded check sent to a source (MEDULLA-FEDERATED-QUERY-R703.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessCheckRequest {
    pub source: String,
    pub principal: String,
    pub record_refs: Vec<String>,
}

/// The source's answer (MEDULLA-FEDERATED-QUERY-R703.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessCheckAnswer {
    pub allowed: Vec<String>,
    pub denied: Vec<String>,
    /// Unix milliseconds.
    pub checked_at: u64,
}

/// Why a hit was dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DropReason {
    Denied,
    Timeout,
    Unavailable,
}

/// A port that answers checks for one or more sources.
pub trait AccessCheckPort {
    /// Answer `request`, reporting how long the source took. `None` means the
    /// source could not answer.
    fn check(&mut self, request: &AccessCheckRequest) -> Option<(AccessCheckAnswer, u64)>;
}

/// One retrieval hit awaiting its check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessHit {
    pub source: String,
    pub record_ref: String,
    pub mode: AccessMode,
}

/// Cached answers keyed by principal, source and record.
#[derive(Debug, Default)]
pub struct AccessCache {
    entries: BTreeMap<(String, String, String), (bool, u64)>,
    ttl_ms: u64,
}

impl AccessCache {
    /// A cache whose entries live `ttl_ms`, capped at [`MAX_CACHE_TTL_MS`].
    pub fn new(ttl_ms: u64) -> Self {
        Self {
            entries: BTreeMap::new(),
            ttl_ms: ttl_ms.min(MAX_CACHE_TTL_MS),
        }
    }

    fn get(&self, principal: &str, hit: &AccessHit, now_ms: u64) -> Option<bool> {
        let key = (
            principal.to_string(),
            hit.source.clone(),
            hit.record_ref.clone(),
        );
        self.entries
            .get(&key)
            .filter(|(_, at)| now_ms.saturating_sub(*at) < self.ttl_ms)
            .map(|(allowed, _)| *allowed)
    }

    fn put(&mut self, principal: &str, source: &str, answer: &AccessCheckAnswer) {
        for (refs, allowed) in [(&answer.allowed, true), (&answer.denied, false)] {
            for record in refs {
                let key = (principal.to_string(), source.to_string(), record.clone());
                self.entries.insert(key, (allowed, answer.checked_at));
            }
        }
    }
}

/// The hits that survive, and each dropped one with its reason.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct FilteredHits {
    pub kept: Vec<AccessHit>,
    pub dropped: Vec<(AccessHit, DropReason)>,
    /// Checks actually sent to ports.
    pub checks_sent: usize,
}

/// Filter hits before bundle compile (MEDULLA-FEDERATED-QUERY-R703.2). Synced hits
/// pass; `checked_at_read` hits keep only what their source allows.
pub fn filter_hits(
    hits: Vec<AccessHit>,
    principal: &str,
    port: &mut dyn AccessCheckPort,
    cache: &mut AccessCache,
    now_ms: u64,
) -> FilteredHits {
    let mut out = FilteredHits::default();
    let mut pending: BTreeMap<String, Vec<AccessHit>> = BTreeMap::new();
    for hit in hits {
        match (hit.mode, cache.get(principal, &hit, now_ms)) {
            (AccessMode::Synced, _) | (_, Some(true)) => out.kept.push(hit),
            (_, Some(false)) => out.dropped.push((hit, DropReason::Denied)),
            (_, None) => pending.entry(hit.source.clone()).or_default().push(hit),
        }
    }
    for (source, hits) in pending {
        for batch in hits.chunks(MAX_RECORDS_PER_CHECK) {
            out.checks_sent += 1;
            check_batch(&source, batch, principal, port, cache, &mut out);
        }
    }
    out
}

fn check_batch(
    source: &str,
    batch: &[AccessHit],
    principal: &str,
    port: &mut dyn AccessCheckPort,
    cache: &mut AccessCache,
    out: &mut FilteredHits,
) {
    let request = AccessCheckRequest {
        source: source.to_string(),
        principal: principal.to_string(),
        record_refs: batch.iter().map(|h| h.record_ref.clone()).collect(),
    };
    let answer = match port.check(&request) {
        None => return drop_all(batch, DropReason::Unavailable, out),
        Some((_, elapsed)) if elapsed > CHECK_TIMEOUT_MS => {
            return drop_all(batch, DropReason::Timeout, out)
        }
        Some((answer, _)) => answer,
    };
    cache.put(principal, source, &answer);
    for hit in batch {
        if answer.allowed.contains(&hit.record_ref) {
            out.kept.push(hit.clone());
        } else {
            out.dropped.push((hit.clone(), DropReason::Denied));
        }
    }
}

fn drop_all(batch: &[AccessHit], reason: DropReason, out: &mut FilteredHits) {
    out.dropped
        .extend(batch.iter().cloned().map(|hit| (hit, reason)));
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: MEDULLA-FEDERATED-QUERY-R703.1
    #[test]
    fn request_round_trips() {
        let request = AccessCheckRequest {
            source: "sharepoint".into(),
            principal: "alice".into(),
            record_refs: vec!["doc-1".into(), "doc-2".into()],
        };
        let json = serde_json::to_string(&request).unwrap();
        assert_eq!(
            serde_json::from_str::<AccessCheckRequest>(&json).unwrap(),
            request
        );
        let answer = AccessCheckAnswer {
            allowed: vec!["doc-1".into()],
            denied: vec!["doc-2".into()],
            checked_at: 1_700_000_000_000,
        };
        let json = serde_json::to_string(&answer).unwrap();
        assert_eq!(
            serde_json::from_str::<AccessCheckAnswer>(&json).unwrap(),
            answer
        );
        assert_eq!(
            serde_json::to_string(&AccessMode::CheckedAtRead).unwrap(),
            "\"checked_at_read\""
        );
        assert!(serde_json::from_str::<AccessMode>("\"lazy\"").is_err());
    }

    struct Port {
        elapsed_ms: u64,
        up: bool,
        requests: Vec<AccessCheckRequest>,
    }

    impl AccessCheckPort for Port {
        fn check(&mut self, request: &AccessCheckRequest) -> Option<(AccessCheckAnswer, u64)> {
            self.requests.push(request.clone());
            if !self.up {
                return None;
            }
            let (allowed, denied) = request
                .record_refs
                .iter()
                .cloned()
                .partition(|r| !r.ends_with("-secret"));
            Some((
                AccessCheckAnswer {
                    allowed,
                    denied,
                    checked_at: 1_000,
                },
                self.elapsed_ms,
            ))
        }
    }

    fn hit(source: &str, record: &str, mode: AccessMode) -> AccessHit {
        AccessHit {
            source: source.into(),
            record_ref: record.into(),
            mode,
        }
    }

    // spec: MEDULLA-FEDERATED-QUERY-R703.2
    #[test]
    fn timeout_drops_hits() {
        let hits = || {
            vec![
                hit("wiki", "w-1", AccessMode::Synced),
                hit("drive", "d-1", AccessMode::CheckedAtRead),
                hit("drive", "d-2-secret", AccessMode::CheckedAtRead),
            ]
        };
        let mut slow = Port {
            elapsed_ms: 2_500,
            up: true,
            requests: vec![],
        };
        let out = filter_hits(
            hits(),
            "alice",
            &mut slow,
            &mut AccessCache::new(300_000),
            1_000,
        );
        assert_eq!(out.kept, vec![hit("wiki", "w-1", AccessMode::Synced)]);
        assert!(out
            .dropped
            .iter()
            .all(|(_, reason)| *reason == DropReason::Timeout));
        assert_eq!(out.dropped.len(), 2);

        let mut fast = Port {
            elapsed_ms: 10,
            up: true,
            requests: vec![],
        };
        let mut cache = AccessCache::new(900_000);
        let out = filter_hits(hits(), "alice", &mut fast, &mut cache, 1_000);
        assert_eq!(out.kept.len(), 2);
        assert_eq!(
            out.dropped,
            vec![(
                hit("drive", "d-2-secret", AccessMode::CheckedAtRead),
                DropReason::Denied
            )]
        );
        assert_eq!(fast.requests.len(), 1, "one check per source");

        // Cached for the principal, but never past 300 s even when asked for longer.
        filter_hits(hits(), "alice", &mut fast, &mut cache, 200_000);
        assert_eq!(fast.requests.len(), 1);
        filter_hits(hits(), "bob", &mut fast, &mut cache, 200_000);
        assert_eq!(
            fast.requests.len(),
            2,
            "another principal is not served from alice's cache"
        );
        filter_hits(hits(), "alice", &mut fast, &mut cache, 301_001);
        assert_eq!(fast.requests.len(), 3);

        let many: Vec<_> = (0..450)
            .map(|i| hit("drive", &format!("d{i}"), AccessMode::CheckedAtRead))
            .collect();
        let mut bulk = Port {
            elapsed_ms: 10,
            up: true,
            requests: vec![],
        };
        let out = filter_hits(many, "alice", &mut bulk, &mut AccessCache::new(0), 0);
        assert_eq!(out.checks_sent, 3);
        assert!(bulk
            .requests
            .iter()
            .all(|r| r.record_refs.len() <= MAX_RECORDS_PER_CHECK));

        let mut down = Port {
            elapsed_ms: 0,
            up: false,
            requests: vec![],
        };
        let out = filter_hits(hits(), "alice", &mut down, &mut AccessCache::new(0), 0);
        assert!(out
            .dropped
            .iter()
            .all(|(_, reason)| *reason == DropReason::Unavailable));
    }
}
