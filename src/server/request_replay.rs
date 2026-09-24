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

#[cfg(any(test, feature = "security"))]
use crate::lock_recovery::LockRecovery;

/// Replay ledger used after a request MAC, time window, and policy claims have
/// verified. The production adapter makes its refusal horizon durable before
/// it returns `Ok(true)`, so the request is dispatched only once a restart can
/// no longer re-admit it.
pub(crate) trait ReplayLedger: Send + Sync {
    /// Atomically record `nonce` of an envelope signed at `timestamp`.
    /// `Ok(false)` means it is (or may be) a replay.
    fn check_and_record(&self, presented: PresentedNonce<'_>) -> Result<bool, String>;
}

/// One nonce as a verified envelope presents it: the nonce, the envelope's
/// SIGNED timestamp, the verifier's clock, and the accepted skew window.
#[derive(Clone, Copy)]
pub(crate) struct PresentedNonce<'a> {
    pub(crate) nonce: &'a str,
    pub(crate) timestamp: u64,
    pub(crate) now: u64,
    pub(crate) window: u64,
}

/// Hard cap on remembered nonces — bounds memory under a misconfigured
/// (excessively large) skew window or a deliberate nonce-flood attempt.
#[cfg(any(test, feature = "security"))]
const MAX_REPLAY_ENTRIES: usize = 200_000;

/// The in-memory half of the ledger: every nonce accepted inside the live
/// skew horizon, plus a `floor` below which NOTHING is accepted.
///
/// The floor is what lets the set be bounded and volatile without weakening
/// protection. Forgetting a nonce is only ever done together with raising the
/// floor over its timestamp (cap eviction), or after its timestamp has left
/// the `2 * window` horizon the skew check already refuses (pruning). A
/// restart forgets everything, so the durable adapter seeds the floor with the
/// high-water timestamp the previous incarnation made durable.
#[cfg(any(test, feature = "security"))]
pub(crate) struct NonceWindow {
    seen: std::collections::HashMap<String, u64>,
    floor: u64,
    last_prune: u64,
}

#[cfg(any(test, feature = "security"))]
impl NonceWindow {
    pub(crate) fn with_floor(floor: u64) -> Self {
        NonceWindow {
            seen: std::collections::HashMap::new(),
            floor,
            last_prune: 0,
        }
    }

    /// `true` when the nonce is accepted; `false` for a replay, or for an
    /// envelope signed at or below the floor (it may have been accepted by a
    /// window this one no longer remembers).
    fn admit(&mut self, presented: PresentedNonce<'_>) -> bool {
        self.prune(presented.now, presented.window);
        if presented.timestamp <= self.floor || self.seen.contains_key(presented.nonce) {
            return false;
        }
        if self.seen.len() >= MAX_REPLAY_ENTRIES {
            self.evict_older_half();
        }
        self.seen
            .insert(presented.nonce.to_string(), presented.timestamp);
        true
    }

    /// Drops entries signed before `now - 2 * window` — at most once per
    /// clock second, so the scan is not paid per request.
    fn prune(&mut self, now: u64, window: u64) {
        if now <= self.last_prune {
            return;
        }
        self.last_prune = now;
        let cutoff = now.saturating_sub(window.saturating_mul(2));
        self.seen.retain(|_, timestamp| *timestamp >= cutoff);
    }

    /// Pathological load outpacing pruning: forget the older half, and raise
    /// the floor over everything forgotten so none of it can be replayed.
    fn evict_older_half(&mut self) {
        let mut entries: Vec<(String, u64)> = self.seen.drain().collect();
        entries.sort_by_key(|(_, timestamp)| *timestamp);
        let keep_from = entries.len() / 2;
        if keep_from > 0 {
            self.floor = self.floor.max(entries[keep_from - 1].1);
        }
        self.seen.extend(entries.into_iter().skip(keep_from));
    }
}

/// Test-build ledger: the same window, no durable floor, so a unit test needs
/// no state dir.
#[cfg(test)]
pub(crate) struct ReplayCache {
    window: std::sync::Mutex<NonceWindow>,
}

#[cfg(test)]
impl ReplayCache {
    pub(crate) fn new() -> Self {
        ReplayCache {
            window: std::sync::Mutex::new(NonceWindow::with_floor(0)),
        }
    }
}

#[cfg(test)]
impl ReplayLedger for ReplayCache {
    fn check_and_record(&self, presented: PresentedNonce<'_>) -> Result<bool, String> {
        Ok(self
            .window
            .lock_recovering("replay nonce cache")
            .admit(presented))
    }
}

#[cfg(feature = "security")]
const REPLAY_TABLE: redb::TableDefinition<&str, u64> =
    redb::TableDefinition::new("verified_request_replay");
/// The one row the durable ledger keeps: the highest envelope timestamp any
/// accepted nonce carried (or will carry — it is written first).
#[cfg(feature = "security")]
const HIGH_WATER_KEY: &str = "accepted_timestamp_high_water";
#[cfg(feature = "security")]
const REPLAY_PHYSICAL_STORE: &str = "epistemic-graph:request-replay";
#[cfg(feature = "security")]
const REPLAY_SCOPE_RESOURCE: &str = "request-replay";
#[cfg(feature = "security")]
const REPLAY_SCOPE_INCARNATION: &str = "request-replay:v1";

/// Durable replay adapter used by secure mode (EH-533).
///
/// Nonces live in a bounded in-memory [`NonceWindow`]; what is durable is a
/// HIGH-WATER envelope timestamp. Before a nonce signed at `t` is accepted,
/// the durable high-water is raised to at least `t` (one kernel commit through
/// `SidecarStore::maintain`, ledgered as maintenance per RF-RULING-005). On
/// reopen the window's floor is that high-water, so every envelope a previous
/// incarnation could have accepted — `t <= high-water` — is refused after a
/// restart or crash, exactly as when every nonce was its own durable row.
///
/// Cost model: timestamps are whole seconds, so the durable commit is paid
/// once per clock second (by the first request that advances it) instead of
/// once per request; the rest take a mutex and a map lookup. The price is
/// availability, not protection: right after a restart, an envelope signed in
/// or before the last second the previous incarnation accepted is refused
/// and must be re-signed (clients sign each attempt with a fresh timestamp).
///
/// **KNOWN GAP — per-node only, NOT replicated across a `cluster`/`raft`
/// deployment** (tracked in `reports/seam-identity-closure.md`, "Raft
/// replay-ledger replication" section; called out by
/// `reports/seam-closure-audit-2026-07-22.md`'s Identity row). This ledger is
/// scoped to ONE node's `EPISTEMIC_GRAPH_SECURITY_STATE_DIR` and is checked
/// entirely BEFORE any Raft/consensus code runs (see `dispatch_inner` in
/// `server/dispatch.rs`). In a multi-node `cluster` deployment a captured,
/// still-signature-valid envelope COULD be replayed once against every node
/// independently within the clock-skew window (`envelope_skew_secs()`,
/// default 300s). Closing this requires routing the check through the Raft
/// log (`crate::raft::ReplicatedMutation`) rather than a local pre-check. The
/// production deployment does not run the `cluster`/`raft` feature, so this
/// gap is not currently exploitable — but MUST be closed before any
/// multi-node `cluster` rollout.
#[cfg(feature = "security")]
pub(crate) struct RedbReplayLedger {
    durable: crate::sidecar_store::SidecarStore<eg_storage::RequestReplayOwner>,
    /// Lock-free fast path: requests signed at or below it need no commit.
    high_water: std::sync::atomic::AtomicU64,
    /// Serializes the commits that ADVANCE the high-water.
    advance: std::sync::Mutex<()>,
    window: std::sync::Mutex<NonceWindow>,
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
        let floor = stored_high_water(&durable)?;
        Ok(RedbReplayLedger {
            durable,
            high_water: std::sync::atomic::AtomicU64::new(floor),
            advance: std::sync::Mutex::new(()),
            window: std::sync::Mutex::new(NonceWindow::with_floor(floor)),
        })
    }

    /// Makes `timestamp` durable as the high-water unless it already is. Only
    /// after this returns may a nonce signed at `timestamp` be accepted.
    fn cover(&self, timestamp: u64) -> Result<(), String> {
        use std::sync::atomic::Ordering;
        if self.high_water.load(Ordering::Acquire) >= timestamp {
            return Ok(());
        }
        let _advancing = self.advance.lock_recovering("replay ledger high-water");
        if self.high_water.load(Ordering::Acquire) >= timestamp {
            return Ok(());
        }
        self.durable
            .maintain("request_replay_high_water", |owner| {
                owner
                    .open_table(REPLAY_TABLE)?
                    .insert(HIGH_WATER_KEY, timestamp)
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            })?;
        self.high_water.store(timestamp, Ordering::Release);
        Ok(())
    }
}

/// The high-water a previous incarnation made durable (0 on a fresh file).
/// Read inside a maintenance write so a fresh file gets its table created.
#[cfg(feature = "security")]
fn stored_high_water(
    durable: &crate::sidecar_store::SidecarStore<eg_storage::RequestReplayOwner>,
) -> Result<u64, String> {
    use redb::ReadableTable;

    let mut stored = 0;
    durable.maintain("request_replay_open", |owner| {
        let table = owner.open_table(REPLAY_TABLE)?;
        stored = table
            .get(HIGH_WATER_KEY)
            .map_err(|e| e.to_string())?
            .map_or(0, |value| value.value());
        Ok(())
    })?;
    Ok(stored)
}

#[cfg(feature = "security")]
impl ReplayLedger for RedbReplayLedger {
    fn check_and_record(&self, presented: PresentedNonce<'_>) -> Result<bool, String> {
        self.cover(presented.timestamp)?;
        Ok(self
            .window
            .lock_recovering("replay ledger window")
            .admit(presented))
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
    Ok(LEDGER.get_or_init(ReplayCache::new))
}

#[cfg(test)]
fn presented(nonce: &str, timestamp: u64, now: u64) -> PresentedNonce<'_> {
    PresentedNonce {
        nonce,
        timestamp,
        now,
        window: 300,
    }
}

#[cfg(test)]
mod window_tests {
    use super::*;

    #[test]
    fn nothing_at_or_below_the_floor_is_accepted() {
        let mut window = NonceWindow::with_floor(1_000);
        assert!(!window.admit(presented("fresh", 1_000, 1_000)));
        assert!(window.admit(presented("fresh", 1_001, 1_001)));
    }

    /// Cap eviction forgets nonces, so it must raise the floor over every one
    /// it forgot: none of them may become replayable.
    #[test]
    fn cap_eviction_raises_the_floor_over_what_it_forgot() {
        let mut window = NonceWindow::with_floor(0);
        let now = 10_000;
        for index in 0..MAX_REPLAY_ENTRIES as u64 {
            let signed_at = now - (MAX_REPLAY_ENTRIES as u64 - index) / 1_000;
            assert!(window.admit(presented(&format!("n{index}"), signed_at, now)));
        }
        assert!(window.admit(presented("overflow", now, now)));
        assert!(window.seen.len() <= MAX_REPLAY_ENTRIES / 2 + 1);
        assert!(
            !window.admit(presented("n0", now - 200, now)),
            "an evicted nonce must stay refused"
        );
        assert!(!window.admit(presented("unseen-but-old", window.floor, now)));
    }
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

    fn ledger(tag: &str) -> (RedbReplayLedger, std::path::PathBuf) {
        let dir = crate::test_support::temp_dir("eg-request-replay", tag);
        let ledger = RedbReplayLedger::open(&dir).expect("open the durable replay ledger");
        (ledger, dir)
    }

    fn accept(ledger: &RedbReplayLedger, nonce: &str, timestamp: u64) -> bool {
        ledger
            .check_and_record(presented(nonce, timestamp, timestamp))
            .expect("check_and_record")
    }

    fn scope_version(ledger: &RedbReplayLedger) -> u64 {
        eg_transaction::version(&ledger.durable.read().unwrap()).unwrap()
    }

    #[test]
    fn a_nonce_is_accepted_once_and_refused_after() {
        let (ledger, dir) = ledger("once");
        assert!(accept(&ledger, "nonce-a", 1_000));
        assert!(!accept(&ledger, "nonce-a", 1_000));
        assert!(accept(&ledger, "nonce-b", 1_000));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// EH-533's proof obligation: nonces are no longer durable rows, yet a
    /// restart must not make an accepted envelope usable again. The reopened
    /// ledger refuses the replay (and anything else signed in that second),
    /// and accepts envelopes signed after it.
    #[test]
    fn an_accepted_nonce_is_refused_after_a_reopen() {
        let dir = crate::test_support::temp_dir("eg-request-replay", "reopen");
        {
            let ledger = RedbReplayLedger::open(&dir).unwrap();
            assert!(accept(&ledger, "early", 990));
            assert!(accept(&ledger, "nonce", 1_000));
        }
        let ledger = RedbReplayLedger::open(&dir).unwrap();
        let replays = [
            ("nonce", 1_000),
            ("early", 990),
            ("unseen-same-second", 1_000),
        ];
        for (nonce, signed_at) in replays {
            assert!(
                !ledger
                    .check_and_record(presented(nonce, signed_at, 1_010))
                    .unwrap(),
                "a restart must not re-admit {nonce}@{signed_at}"
            );
        }
        assert!(accept(&ledger, "after-restart", 1_001));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The throughput half: requests signed in one second share ONE durable
    /// commit instead of paying one each.
    #[test]
    fn one_durable_commit_per_advancing_second() {
        let (ledger, dir) = ledger("group");
        let opened = scope_version(&ledger);
        for index in 0..50 {
            assert!(accept(&ledger, &format!("same-second-{index}"), 1_000));
        }
        assert_eq!(scope_version(&ledger), opened + 1);
        assert!(accept(&ledger, "next-second", 1_001));
        assert!(accept(&ledger, "late-but-in-window", 999));
        assert_eq!(scope_version(&ledger), opened + 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Past `2 * window` a nonce could never pass the timestamp-skew check
    /// anyway, so pruning forgets it.
    #[test]
    fn entries_older_than_two_windows_are_pruned() {
        let (ledger, dir) = ledger("prune");
        assert!(accept(&ledger, "old", 1_000));
        let later = 1_000 + 300 * 2 + 1;
        assert!(accept(&ledger, "other", later));
        assert!(!ledger.window.lock().unwrap().seen.contains_key("old"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// REVIEW-root-c2-6bee2377 P0-1, the auth half: concurrent distinct nonces
    /// must all be accepted — including the ones racing the high-water commit.
    #[test]
    fn concurrent_distinct_nonces_are_all_accepted() {
        let (ledger, dir) = ledger("concurrent");
        let ledger = Arc::new(ledger);
        let requests = 8;
        let accepted: usize = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..requests)
                .map(|index| {
                    let ledger = Arc::clone(&ledger);
                    scope.spawn(move || accept(&ledger, &format!("nonce-{index}"), 1_000 + index))
                })
                .collect();
            handles
                .into_iter()
                .filter_map(|handle| handle.join().unwrap().then_some(()))
                .count()
        });
        assert_eq!(
            accepted, requests as usize,
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
                    scope.spawn(move || accept(&ledger, "same", 1_000))
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
