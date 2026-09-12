//! The signed-request replay ledger: one node's durable record of which
//! envelope nonces it has already accepted.
//!
//! Split out of `server::auth` so the DURABLE adapter has a home its own tests
//! can reach. Inside `auth` the whole family was `#[cfg(test)]`-swapped for an
//! in-memory cache, so [`RedbReplayLedger`] — the one that runs in production,
//! on the hot path of every signed request — had zero test coverage.
//!
//! This is a transport envelope-nonce ledger, consulted in `verify_request`
//! BEFORE dispatch. It is NOT the mutation kernel's operation-replay authority:
//! that one decides whether a MUTATION replays inside a store scope, this one
//! whether a signed REQUEST (including a read) has been seen at all. They
//! answer different questions over different identities, so they are not two
//! authorities over one decision (RF-RULING-004).

use crate::lock_recovery::LockRecovery;

/// Replay ledger used after a request MAC, time window, and policy claims have
/// verified. The production adapter is durable and commits before dispatch.
pub(crate) trait ReplayLedger: Send + Sync {
    /// Atomically record a nonce. `Ok(false)` means it was already present.
    fn check_and_record(&self, nonce: &str, now: u64, window: u64) -> Result<bool, String>;
}

#[cfg(test)]
pub(crate) struct ReplayCache {
    pub(crate) seen: std::sync::Mutex<std::collections::HashMap<String, u64>>,
}

/// Hard cap on cached nonces — bounds memory under a misconfigured
/// (excessively large) skew window or a deliberate nonce-flood attempt.
#[cfg(test)]
const MAX_REPLAY_ENTRIES: usize = 200_000;

#[cfg(test)]
impl ReplayCache {
    /// Returns `true` if `nonce` is accepted (not seen before within the
    /// retention horizon); `false` if it is a replay. Always prunes entries
    /// older than `2 * window` first (anything older could never pass the
    /// timestamp-skew check anyway, so retaining it further gains nothing).
    fn check_and_record_memory(&self, nonce: &str, now: u64, window: u64) -> bool {
        let mut seen = self.seen.lock_recovering("replay nonce cache");
        let cutoff = now.saturating_sub(window.saturating_mul(2));
        seen.retain(|_, ts| *ts >= cutoff);
        if seen.contains_key(nonce) {
            return false;
        }
        if seen.len() >= MAX_REPLAY_ENTRIES {
            // Extremely defensive fallback for a pathological configuration
            // that outpaces normal pruning: drop the older half rather than
            // growing without bound.
            let mut entries: Vec<(String, u64)> = seen.drain().collect();
            entries.sort_by_key(|(_, ts)| *ts);
            let keep_from = entries.len() / 2;
            seen.extend(entries.into_iter().skip(keep_from));
        }
        seen.insert(nonce.to_string(), now);
        true
    }
}

#[cfg(test)]
impl ReplayLedger for ReplayCache {
    fn check_and_record(&self, nonce: &str, now: u64, window: u64) -> Result<bool, String> {
        Ok(self.check_and_record_memory(nonce, now, window))
    }
}

#[cfg(feature = "security")]
const REPLAY_TABLE: redb::TableDefinition<&str, u64> =
    redb::TableDefinition::new("verified_request_replay");
#[cfg(feature = "security")]
const REPLAY_PHYSICAL_STORE: &str = "epistemic-graph:request-replay";
#[cfg(feature = "security")]
const REPLAY_SCOPE_RESOURCE: &str = "request-replay";
#[cfg(feature = "security")]
const REPLAY_SCOPE_INCARNATION: &str = "request-replay:v1";

/// Durable replay adapter used by secure mode. A successful `check_and_record`
/// commits before the request is dispatched, so a process restart cannot make
/// a previously accepted nonce usable again; durability itself belongs to the
/// kernel (`eg_transaction::MutationKernel::commit`, reached through
/// `sidecar_store::SidecarStore::maintain`), not a `redb::Durability` this
/// file sets on its own transaction.
///
/// **KNOWN GAP — per-node only, NOT replicated across a `cluster`/`raft`
/// deployment** (tracked in `reports/seam-identity-closure.md`, "Raft
/// replay-ledger replication" section; called out by
/// `reports/seam-closure-audit-2026-07-22.md`'s Identity row). This ledger is
/// a local kernel-owned owner file scoped to ONE node's
/// `EPISTEMIC_GRAPH_SECURITY_STATE_DIR`. It is checked entirely BEFORE any
/// Raft/consensus code runs (see `dispatch_inner` in `server/dispatch.rs`,
/// which calls `verify_request_with_security_dir` — and therefore this
/// ledger — before any `#[cfg(feature = "raft")]` code executes). In a
/// hypothetical multi-node `cluster` deployment, a captured, still-
/// signature-valid signed envelope COULD be replayed once against every node
/// independently within the clock-skew window (`envelope_skew_secs()`,
/// default 300s), because each node's `seen`-nonce set is disjoint. Closing
/// this requires routing the nonce check-and-record through the SAME
/// Raft-log consensus path ordinary mutations use
/// (`crate::raft::ReplicatedMutation` / `NativeMutationCommand` in
/// `src/raft/mod.rs`) rather than a purely local pre-check — a genuine new
/// integration point on the hot path of EVERY authenticated request
/// (including reads), not merely "replicate existing state." As of this
/// writing the homelab's production `epistemic-graph` deployment does not run
/// the `cluster`/`raft` feature at all (the default/`full` build links no
/// `openraft`; see this crate's `Cargo.toml` `cluster` feature and the seam
/// audit's Placement-seam finding), so this gap is not currently exploitable
/// in production — but MUST be closed before any multi-node `cluster` rollout.
#[cfg(feature = "security")]
pub(crate) struct RedbReplayLedger {
    durable: crate::sidecar_store::SidecarStore<eg_storage::RequestReplayOwner>,
    last_prune: std::sync::Mutex<u64>,
}

#[cfg(feature = "security")]
impl RedbReplayLedger {
    pub(crate) fn open(dir: &std::path::Path) -> Result<Self, String> {
        let path = dir.join("request-replay.redb");
        let durable = crate::sidecar_store::SidecarStore::open(
            &path,
            REPLAY_PHYSICAL_STORE,
            REPLAY_SCOPE_RESOURCE,
            REPLAY_SCOPE_INCARNATION,
            crate::store_authority::process_authority(),
        )?;
        Ok(RedbReplayLedger {
            durable,
            last_prune: std::sync::Mutex::new(0),
        })
    }
}

#[cfg(feature = "security")]
impl ReplayLedger for RedbReplayLedger {
    /// The prune scan and the nonce check-and-insert are ONE logical
    /// operation, so both happen inside ONE `maintain` call rather than a
    /// call per row. `maintain` bumps the scope version on every call
    /// (RF-RULING-005), which is the intent here, not a side effect: this
    /// nonce ledger carries no caller identity, but it is still ledgered like
    /// every other owner write, so a replay-refusal decision becomes an
    /// auditable, versioned fact instead of an un-ledgered side channel.
    fn check_and_record(&self, nonce: &str, now: u64, window: u64) -> Result<bool, String> {
        use redb::ReadableTable;

        let should_prune = {
            let mut last = self.last_prune.lock_recovering("replay ledger prune clock");
            if now.saturating_sub(*last) >= window {
                *last = now;
                true
            } else {
                false
            }
        };
        let mut accepted = false;
        self.durable
            .maintain("request_replay_check_and_record", |owner| {
                let mut table = owner.open_table(REPLAY_TABLE)?;
                if should_prune {
                    let cutoff = now.saturating_sub(window.saturating_mul(2));
                    let mut expired = Vec::new();
                    for row in table.iter().map_err(|e| e.to_string())? {
                        let (key, timestamp) = row.map_err(|e| e.to_string())?;
                        if timestamp.value() < cutoff {
                            expired.push(key.value().to_string());
                        }
                    }
                    for key in expired {
                        table.remove(key.as_str()).map_err(|e| e.to_string())?;
                    }
                }
                if table.get(nonce).map_err(|e| e.to_string())?.is_some() {
                    return Ok(());
                }
                table.insert(nonce, now).map_err(|e| e.to_string())?;
                accepted = true;
                Ok(())
            })?;
        Ok(accepted)
    }
}

#[cfg(all(feature = "security", not(test)))]
pub(crate) fn durable_replay_ledger(
    state_dir: Option<&str>,
) -> Result<&'static RedbReplayLedger, String> {
    static LEDGER: std::sync::OnceLock<Result<RedbReplayLedger, String>> =
        std::sync::OnceLock::new();
    match LEDGER.get_or_init(|| {
        let dir = std::env::var("EPISTEMIC_GRAPH_SECURITY_STATE_DIR")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .or_else(|| state_dir.map(str::to_string))
            .ok_or_else(|| {
                "secure request context requires EPISTEMIC_GRAPH_SECURITY_STATE_DIR or a persist directory".to_string()
            })?;
        RedbReplayLedger::open(std::path::Path::new(&dir))
    }) {
        Ok(ledger) => Ok(ledger),
        Err(message) => Err(message.clone()),
    }
}

#[cfg(all(not(feature = "security"), not(test)))]
pub(crate) fn durable_replay_ledger(
    _state_dir: Option<&str>,
) -> Result<&'static dyn ReplayLedger, String> {
    Err("secure request context requires the security feature".to_string())
}

#[cfg(test)]
pub(crate) fn durable_replay_ledger(
    _state_dir: Option<&str>,
) -> Result<&'static dyn ReplayLedger, String> {
    static LEDGER: std::sync::OnceLock<ReplayCache> = std::sync::OnceLock::new();
    Ok(LEDGER.get_or_init(|| ReplayCache {
        seen: std::sync::Mutex::new(std::collections::HashMap::new()),
    }))
}

/// The DURABLE ledger, exercised directly. `durable_replay_ledger` is
/// `#[cfg(test)]`-swapped for the in-memory cache so a test needs no state dir,
/// which left [`RedbReplayLedger`] — the adapter that actually runs in
/// production — with zero coverage. These drive it as `verify_envelope_v2_with`
/// does.
#[cfg(all(test, feature = "security"))]
mod durable_tests {
    use super::*;
    use std::sync::Arc;

    const WINDOW: u64 = 300;

    fn ledger(tag: &str) -> (RedbReplayLedger, std::path::PathBuf) {
        let dir = crate::test_support::temp_dir("eg-request-replay", tag);
        let ledger = RedbReplayLedger::open(&dir).expect("open the durable replay ledger");
        (ledger, dir)
    }

    #[test]
    fn a_nonce_is_accepted_once_and_refused_after() {
        let (ledger, dir) = ledger("once");
        assert!(ledger.check_and_record("nonce-a", 1_000, WINDOW).unwrap());
        assert!(!ledger.check_and_record("nonce-a", 1_000, WINDOW).unwrap());
        assert!(ledger.check_and_record("nonce-b", 1_000, WINDOW).unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The record is durable, not process state: reopening the same file must
    /// still refuse a nonce accepted before the restart. This is the property
    /// the whole adapter exists for.
    #[test]
    fn an_accepted_nonce_survives_a_reopen() {
        let dir = crate::test_support::temp_dir("eg-request-replay", "reopen");
        {
            let ledger = RedbReplayLedger::open(&dir).unwrap();
            assert!(ledger.check_and_record("nonce", 1_000, WINDOW).unwrap());
        }
        let ledger = RedbReplayLedger::open(&dir).unwrap();
        assert!(
            !ledger.check_and_record("nonce", 1_000, WINDOW).unwrap(),
            "a restart must not make an accepted nonce usable again"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Past `2 * window` a nonce could never pass the timestamp-skew check
    /// anyway, so the prune scan drops it and the key becomes reusable.
    #[test]
    fn entries_older_than_two_windows_are_pruned() {
        let (ledger, dir) = ledger("prune");
        assert!(ledger.check_and_record("old", 1_000, WINDOW).unwrap());
        let later = 1_000 + WINDOW * 2 + 1;
        assert!(
            ledger.check_and_record("old", later, WINDOW).unwrap(),
            "an entry past 2 * window must have been pruned"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// REVIEW-root-c2-6bee2377 P0-1, the auth half. Every `check_and_record`
    /// goes through `SidecarStore::maintain`, whose batch id used to be
    /// `{event}:v{version}` over a version read OUTSIDE the write transaction.
    /// Two concurrent signed requests observed the same version, built
    /// BYTE-IDENTICAL batches, and the loser took the replay path with
    /// `accepted` still `false` — so `verify_request` rejected a VALID request
    /// with "nonce already used (replay rejected)", on the hot path of every
    /// signed request. Distinct nonces must all be accepted.
    #[test]
    fn concurrent_distinct_nonces_are_all_accepted() {
        let (ledger, dir) = ledger("concurrent");
        let ledger = Arc::new(ledger);
        let requests = 8;
        let accepted: usize = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..requests)
                .map(|index| {
                    let ledger = Arc::clone(&ledger);
                    scope.spawn(move || {
                        ledger
                            .check_and_record(&format!("nonce-{index}"), 1_000, WINDOW)
                            .expect("a concurrent check_and_record must not fail")
                    })
                })
                .collect();
            handles
                .into_iter()
                .filter_map(|handle| handle.join().unwrap().then_some(()))
                .count()
        });
        assert_eq!(
            accepted, requests,
            "a valid concurrent signed request was rejected as a replay"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The other side of the same race: ONE nonce presented concurrently is a
    /// genuine replay, and exactly one presentation may be accepted.
    #[test]
    fn concurrent_uses_of_one_nonce_accept_exactly_one() {
        let (ledger, dir) = ledger("replay");
        let ledger = Arc::new(ledger);
        let accepted: usize = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let ledger = Arc::clone(&ledger);
                    scope.spawn(move || ledger.check_and_record("same", 1_000, WINDOW).unwrap())
                })
                .collect();
            handles
                .into_iter()
                .filter_map(|handle| handle.join().unwrap().then_some(()))
                .count()
        });
        assert_eq!(
            accepted, 1,
            "a replayed nonce must be accepted exactly once"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
