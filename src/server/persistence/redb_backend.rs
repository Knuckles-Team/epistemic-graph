//! redb write-through persistence backend (CONCEPT:EG-KG.storage.kg-kg, feature `redb`).
//!
//! The authoritative tier commits every graph mutation into an embedded
//! canonical `redb` shard database (`{persist_dir}/graph-<n>.redb`), keyed by a
//! `(graph, …)` prefix so
//! all tenants share the same tables (not a file per graph). The in-memory graph is
//! a bounded resident projection with durability-gated read-through eviction.
//!
//! ## The #1 risk: never one commit per mutation
//!
//! A shard commit is a B-tree + WAL + fsync — orders of magnitude more expensive
//! than a row write, so one per graph mutation would collapse write p99. A
//! dedicated OS thread therefore owns the file's
//! [`Shard`](crate::redb_store::shard::Shard), drains a bounded channel, and
//! folds MANY mutations into ONE admitted scope group per group-commit boundary.
//!
//! ## Every shard commit is `Durability::Immediate`, and there is no runtime knob
//!
//! The level is not this backend's to choose any more: `eg_storage`'s
//! `physical::root::WRITE_DURABILITY` is a CONSTANT `Durability::Immediate` on
//! the one `begin_write` every kernel mutation takes, because a weaker level
//! would let redb roll a committed ledger back on crash — un-consuming an
//! acknowledged replay nonce and re-enabling the double apply. Group commit still
//! folds N ops into ONE Immediate fsync, while every batch pays that real fsync at
//! the group-commit boundary.
//!
//! Backpressure is bounded and lossless: when the writer queue is full, producers
//! wait for capacity rather than shedding persistence work. Authoritative batch
//! callers enqueue from Tokio's blocking pool and await durable completion.
//!
//! ## Tables (all keyed by graph prefix)
//!   * `nodes`          `(graph, id)            -> node properties msgpack`
//!   * `edges`          `(graph, src, tgt, ord) -> edge properties msgpack`
//!   * `ledger`         `(graph, seq)           -> ledger line`
//!   * `semantic_store` `graph                  -> semantic store blob (msgpack)`
//!   * `graph_meta`     `graph                  -> {name, graph_type} blob` (replaces manifest.json)

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use redb::{ReadableTable, TableDefinition};
use tokio::sync::RwLock;

use crate::graph::GraphCore;
use crate::redb_store::GraphDump;

#[cfg(test)]
use crate::redb_layout::shard_filename;
#[cfg(test)]
use crate::server::ServerState;
#[cfg(test)]
use crate::{mutation_batch::*, protocol::*};
#[cfg(test)]
use tokio::sync::oneshot;

#[cfg(test)]
use super::PersistenceBackend;

// The graph table layout + the PURE durable-row machinery (Method→rows apply,
// group-commit, checkpoint/load) now live in the server-INDEPENDENT
// `crate::redb_store` (CONCEPT:EG-KG.backend.engine-modes) so the embedded API can drive the SAME
// durable format with no Tokio. This backend reuses them verbatim — ONE format,
// never duplicated — and adds only the off-reactor group-commit writer thread +
// the `PersistenceBackend` async trait wiring on top.
use crate::redb_store::shard::Shard;

#[cfg(test)]
use eg_transaction::OutboxClaimBudget;

mod backend_audit;
mod backend_backup;
mod backend_load;
mod backend_open;
mod backend_routing;
mod backend_shard_routes;
mod commands;
#[cfg(any(feature = "compute-dist", feature = "matview"))]
mod matview_api;
#[cfg(feature = "raft")]
mod raft_api;
#[cfg(all(test, feature = "raft"))]
mod raft_linger_tests;
mod shard_writer;
#[cfg(feature = "tsdb")]
mod timeseries;
mod trait_capabilities;
mod trait_envelopes;
mod trait_graph;
mod trait_graph_reads;
mod trait_impl;
mod trait_mutations;
mod trait_native;
mod trait_outbox;
mod writer_commands;
mod writer_thread;
#[cfg(feature = "raft")]
mod xshard_api;

pub(crate) use commands::{
    ChangeEnvelopePayload, ChangeEnvelopesPayload, Cmd, CrossModalBatchPayload, CrossModalPayload,
    MutationBatchPayload, PageQuery,
};
pub(crate) use shard_writer::shard_index;
use shard_writer::{join_blocking_in_order, resolve_flush_threshold, resolve_shard_count};
use shard_writer::{PreparedShard, ShardWriter};
#[cfg(feature = "tsdb")]
pub use timeseries::TsReconcileReport;
#[cfg(feature = "security")]
use writer_thread::in_control_write;
use writer_thread::run;
/// `(first, last)` present Raft log index for a group, or an error (CONCEPT:EG-KG.storage.one-fsync-covers-raft).
type LogBoundsResult = Result<(Option<u64>, Option<u64>), String>;

/// Per-ATTEMPT identity for one shard write this backend admits.
///
/// The kernel resolves a batch id that already carries a durable receipt to
/// `Begin::Replay` and SKIPS it, so a reused id would silently drop a drain after
/// a restart. Hence `(pid, one nonce per process, counter)` — never derived from
/// `(raft_group, index)` or any caller key; see `redb_store::shard::drain_batch`.
fn shard_write_attempt_id(label: &str) -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    static NONCE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let nonce = *NONCE.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_nanos() as u64)
            .unwrap_or(0)
    });
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{label}/{}-{nonce}:{seq}", std::process::id())
}
const MAX_DURABLE_SEMANTIC_BYTES: usize = 384 * 1024 * 1024;
const MAX_DURABLE_SEMANTIC_ITEMS: usize = 4_000_000;

fn decode_durable_semantic(
    bytes: &[u8],
) -> Result<crate::compute::semantic::SemanticStore, String> {
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_DURABLE_SEMANTIC_BYTES,
            MAX_DURABLE_SEMANTIC_ITEMS,
            64,
        ),
    )
    .map_err(|_| "stored semantic index is invalid".to_string())
}
// Per-group Raft metadata (vote, applied-state pointers, last-purged), keyed by
// `(group_id, key)`. Lives in the authoritative shard alongside the log; Raft-only, so it
// stays here with the Raft helpers rather than in the shared graph store.
pub(crate) const RAFT_META: TableDefinition<(u64, &str), &[u8]> = TableDefinition::new("raft_meta");

/// Encryption-at-rest key-mismatch canary (GOC-16 / BUG-248). The table carries one
/// AEAD-sealed canary row plus one non-secret, versioned key-binding row. The sealed
/// plaintext includes the binding, so changing the plaintext metadata alone cannot
/// make a different key reference appear valid. `ShardWriter::open` verifies both
/// before spawning a writer thread or binding a listener.
///
/// The table is copied verbatim by online backup, offline shard migration, and
/// restore. A backup therefore preserves the key identity/version boundary; restore
/// never silently re-establishes a canary under whatever key happens to be present.
/// A changed key reference is an explicit rotation boundary and fails closed until
/// the documented offline re-encryption ceremony has completed.
pub(crate) const ENCRYPTION_CANARY: TableDefinition<&str, &[u8]> =
    TableDefinition::new("encryption_canary");

#[cfg(feature = "security")]
const ENCRYPTION_CANARY_KEY: &str = "v1";
#[cfg(feature = "security")]
pub(crate) const ENCRYPTION_KEY_BINDING_KEY: &str = "key-binding-v1";
#[cfg(feature = "security")]
const ENCRYPTION_CANARY_PLAINTEXT: &[u8] = b"epistemic-graph-encryption-canary";

/// What [`verify_or_establish_encryption_canary`] did, so the open path can log the
/// TRUTH rather than one undifferentiated "ENABLED" line (BUG-PE-055).
#[cfg(feature = "security")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CanaryOutcome {
    /// The store's existing canary decrypted under the configured key.
    Verified,
    /// A pre-NE-028 fixed canary was verified and upgraded to the bound pair.
    UpgradedLegacy,
    /// FIRST use of a key on an EMPTY store — a new canary was written. On a store
    /// that already held rows this is refused, not logged.
    Established,
}

/// Does this shard already hold durable rows (BUG-PE-055)?
///
/// The question the canary path could not previously ask. Establishing a canary is
/// only safe on an EMPTY store: on a populated PLAINTEXT store it silently converts
/// every subsequent write to sealed framing while every pre-existing value stays
/// plaintext, so each of those reads then fails closed with
/// `"encrypted durable value is missing sealed framing"`. `crypto`'s
/// [`TXN_RECOVERY_KEY_ENV`](crate::crypto::TXN_RECOVERY_KEY_ENV) doc already names that
/// as "a destructive-read operation on a populated plaintext store, not a config
/// toggle"; this is the probe that lets the open path refuse it.
/// Asked of the file's CATALOG, which is the only reader class that exists before
/// any graph scope is bound — and this runs at open, before one is. That is not a
/// weakening: `graph_meta` is file-wide, and every graph that receives a row gets
/// its catalog entry backfilled in the SAME admitted group as those rows
/// (`redb_store::backfill_graph_meta_row`), so a shard with rows always has a
/// catalog entry. The one direction this can be wrong — a registered graph that
/// never received a row — refuses to establish a canary on a store that is empty
/// in fact, which is the safe direction to be wrong in.
#[cfg(feature = "security")]
fn shard_holds_durable_rows(shard: &Shard) -> Result<bool, String> {
    let control = shard.control_read()?;
    let catalog = control.open_owner_table(crate::redb_store::GRAPH_META)?;
    let has_rows = {
        let mut rows = catalog.iter().map_err(|e| e.to_string())?;
        match rows.next() {
            None => false,
            Some(row) => {
                row.map_err(|e| e.to_string())?;
                true
            }
        }
    };
    Ok(has_rows)
}

/// Does this shard carry encryption-at-rest metadata (BUG-PE-055)?
///
/// Answered on the NO-KEY path, which previously never looked: a store whose values
/// are sealed opened cleanly with no key at all and then failed per-read. Startup is
/// where that belongs.
#[cfg(feature = "security")]
fn shard_carries_encryption_metadata(shard: &Shard) -> Result<bool, String> {
    let control = shard.control_read()?;
    let table = control.open_owner_table(ENCRYPTION_CANARY)?;
    for key in [ENCRYPTION_CANARY_KEY, ENCRYPTION_KEY_BINDING_KEY] {
        if table.get(key).map_err(|e| e.to_string())?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Verify (or, on first use, establish) that `cipher` is the SAME key and stable
/// identity/version that sealed this shard's previous data. Called from
/// `ShardWriter::open` before any writer thread spawns or any listener binds.
///
/// A pre-key-lifecycle store may have only the original fixed plaintext canary. That
/// legacy row is verified and upgraded atomically with the new binding record. A
/// store with a binding row but no canary (or vice versa) is treated as tampered and
/// fails closed; neither half is ever silently recreated.
#[cfg(feature = "security")]
fn plan_encryption_canary(
    shard: &Shard,
    cipher: &crate::crypto::ValueCipher,
) -> Result<CanaryPlan, String> {
    let (existing_canary, existing_binding) = {
        let control = shard.control_read()?;
        let table = control.open_owner_table(ENCRYPTION_CANARY)?;
        let canary = table
            .get(ENCRYPTION_CANARY_KEY)
            .map_err(|e| e.to_string())?
            .map(|v| v.value().to_vec());
        let binding = table
            .get(ENCRYPTION_KEY_BINDING_KEY)
            .map_err(|e| e.to_string())?
            .map(|v| v.value().to_vec());
        (canary, binding)
    };

    let configured_ref = cipher.key_ref();
    let expected_plaintext = crate::crypto::encryption_canary_plaintext(configured_ref);
    match (existing_canary, existing_binding) {
        (Some(sealed), Some(binding_bytes)) => {
            let binding = crate::crypto::EncryptionKeyBinding::decode(&binding_bytes)?;
            let persisted_ref = binding.key_ref()?;
            if &persisted_ref != configured_ref {
                return Err(format!(
                    "refusing to open the durable graph store: configured encryption key \
                     reference {}@{} does not match persisted reference {}@{}; complete \
                     the documented offline key-rotation procedure before changing it",
                    configured_ref.id,
                    configured_ref.version,
                    persisted_ref.id,
                    persisted_ref.version,
                ));
            }
            let plaintext = cipher.unseal(&sealed).map_err(|_| {
                format!(
                    "refusing to open the durable graph store: {} does not match the key \
                     that previously encrypted this store (canary decryption failed)",
                    crate::crypto::ENCRYPTION_KEY_ENV,
                )
            })?;
            if plaintext != expected_plaintext {
                return Err(
                    "refusing to open the durable graph store: encryption canary binding \
                     is invalid or tampered"
                        .to_string(),
                );
            }
            Ok(CanaryPlan::Verified(CanaryOutcome::Verified))
        }
        (Some(sealed), None) => {
            // Upgrade the pre-NE-028 fixed canary only after proving the configured
            // key can decrypt it. The binding is then written in the SAME transaction
            // as the new authenticated canary, so a crash leaves either the legacy
            // row (safe to retry) or the complete new pair.
            let plaintext = cipher.unseal(&sealed).map_err(|_| {
                format!(
                    "refusing to open the durable graph store: {} does not match the key \
                     that previously encrypted this store (legacy canary decryption failed)",
                    crate::crypto::ENCRYPTION_KEY_ENV,
                )
            })?;
            if plaintext != ENCRYPTION_CANARY_PLAINTEXT {
                return Err(
                    "refusing to open the durable graph store: legacy encryption canary is \
                     invalid or tampered"
                        .to_string(),
                );
            }
            let binding = crate::crypto::EncryptionKeyBinding::from_key_ref(configured_ref);
            let binding_bytes = binding.encode()?;
            let sealed = cipher.seal(&expected_plaintext);
            Ok(CanaryPlan::Write {
                sealed,
                binding_bytes,
                outcome: CanaryOutcome::UpgradedLegacy,
            })
        }
        (None, Some(_)) => Err(
            "refusing to open the durable graph store: encryption key binding exists but \
             its canary is missing"
                .to_string(),
        ),
        (None, None) => {
            // BUG-PE-055: this arm used to be taken IDENTICALLY for a brand-new store
            // and for a populated store that was written in plaintext — it established
            // a canary and started encrypting, silently, logging only
            // "redb encryption-at-rest ENABLED". Every pre-existing value then became
            // unreadable one read at a time. Establishing a canary is only ever
            // correct on an EMPTY store.
            if shard_holds_durable_rows(shard)? {
                return Err(format!(
                    "refusing to open the durable graph store: {} is set but this store \
                     carries no encryption canary and already holds durable rows, so it \
                     was written in PLAINTEXT. Enabling encryption-at-rest on a populated \
                     plaintext store is a destructive-read operation, not a config \
                     toggle: every existing value would fail to unseal. Complete the \
                     documented offline re-encryption procedure into a fresh persist \
                     dir, or unset {} to keep serving this store as it is.",
                    crate::crypto::ENCRYPTION_KEY_ENV,
                    crate::crypto::ENCRYPTION_KEY_ENV,
                ));
            }
            let binding = crate::crypto::EncryptionKeyBinding::from_key_ref(configured_ref);
            let binding_bytes = binding.encode()?;
            let sealed = cipher.seal(&expected_plaintext);
            Ok(CanaryPlan::Write {
                sealed,
                binding_bytes,
                outcome: CanaryOutcome::Established,
            })
        }
    }
}

/// What [`plan_encryption_canary`] decided this store needs, WITHOUT doing it.
///
/// The two halves are separate because the decision is per-STORE while the
/// refusal is per-PERSIST-DIR. A dir holds K shard files, each with its own
/// canary table and its own local view of "am I empty?", and `open` refuses the
/// whole dir if ANY of them refuses. While establishing the canary happened
/// inside the per-shard decision, a refusal on the one shard that held the data
/// still left every OTHER shard — genuinely empty, and so genuinely eligible to
/// have a canary established — durably converted to a key-required store.
///
/// The dir was then bricked in both directions: it would not open WITH the key
/// (the data shard refuses "this store was written in PLAINTEXT") and would not
/// open WITHOUT it (the converted shards refuse "this store carries
/// encryption-at-rest metadata"), which makes the first refusal's own documented
/// remediation -- "unset EPISTEMIC_GRAPH_ENCRYPTION_KEY to keep serving this
/// store as it is" -- false. K is greater than one by default in a served
/// process (`resolve_shard_count` autosizes from CPUs, and under Raft K == N
/// groups), so this was the ordinary case, not an edge one.
///
/// Planning first makes the refusal byte-clean: the open path plans every shard,
/// and only once every shard has agreed does it apply any plan.
#[cfg(feature = "security")]
enum CanaryPlan {
    /// This store already carries a canary this key verifies, or carries none
    /// and needs none. Nothing to write.
    Verified(CanaryOutcome),
    /// This store needs its canary written before it can be served.
    Write {
        sealed: Vec<u8>,
        binding_bytes: Vec<u8>,
        outcome: CanaryOutcome,
    },
}

/// Commit a [`CanaryPlan`], after every store in the persist dir has agreed.
#[cfg(feature = "security")]
fn apply_encryption_canary(shard: &Shard, plan: CanaryPlan) -> Result<CanaryOutcome, String> {
    match plan {
        CanaryPlan::Verified(outcome) => Ok(outcome),
        CanaryPlan::Write {
            sealed,
            binding_bytes,
            outcome,
        } => {
            write_encryption_canary(shard, &sealed, &binding_bytes)?;
            Ok(outcome)
        }
    }
}

/// Write the sealed canary and its key-binding row as ONE control-only commit, so
/// a crash leaves the store's previous state or the complete new pair, never half.
/// Control-only because `encryption_canary` is file-wide: it belongs to the file,
/// not to any graph it hosts.
#[cfg(feature = "security")]
fn write_encryption_canary(
    shard: &Shard,
    sealed: &[u8],
    binding_bytes: &[u8],
) -> Result<(), String> {
    in_control_write(shard, "encryption_canary", |write| {
        let mut table = write.control().open_table(ENCRYPTION_CANARY)?;
        table
            .insert(ENCRYPTION_CANARY_KEY, sealed)
            .map_err(|e| e.to_string())?;
        table
            .insert(ENCRYPTION_KEY_BINDING_KEY, binding_bytes)
            .map(|_| ())
            .map_err(|e| e.to_string())
    })
}

/// Adaptive group-commit micro-linger tuning for the redb writer (CONCEPT:EG-KG.backend.adaptive-linger-coalesce).
///
/// Live profiling of the `eg-redb-writer` thread showed it pinned ~100% on ext4
/// writeback (disk ~83% util, ~50ms write latency, queue depth 42) while every
/// tokio worker sat idle — it is the ingestion write ceiling. The machinery already
/// group-commits (many `Cmd::Mutation` fold into ONE `WriteTransaction`/fsync), but
/// because every authoritative write carries a commit-before-ack `done` oneshot,
/// `Pending::has_barrier()` is ALWAYS true, so the writer commits the instant the
/// channel momentarily drains. With low in-flight write concurrency (serial awaits —
/// the idle workers) the batch is whatever incidentally sat in the channel, i.e.
/// ~1 op ⇒ ~1 fsync per write. The batching machinery was starved of a window.
///
/// This adds a bounded, adaptive linger: when about to commit a SHALLOW barrier
/// batch, spend ONE `recv_timeout(linger)` letting more concurrent writers arrive,
/// then drain again. "Shallow" counts both graph mutations and Raft log entries:
/// both are rows in the same pending durable transaction, so excluding a raft-only
/// append would restore the pathological one-append/one-fsync shape EH-290 measured.
/// It MIRRORS the in-memory write-coalescer's `max_linger`
/// (CONCEPT:EG-KG.sharding.per-graph-write-coalescer, `write_coalescer.rs`) but for the DURABLE tier — it does NOT
/// touch the coalescer. Durability is unchanged: authoritative writes still commit
/// `Durability::Immediate` BEFORE their `done` fires; we only widen the batch, never
/// defer an ack past its commit. A crash before commit still loses only un-acked writes.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct RedbGroupCommitTestControl {
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}

#[cfg(test)]
impl RedbGroupCommitTestControl {
    pub(crate) fn new() -> (
        Arc<Self>,
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (entered, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release, release_rx) = std::sync::mpsc::sync_channel(1);
        (
            Arc::new(Self {
                entered,
                release: std::sync::Mutex::new(Some(release_rx)),
            }),
            entered_rx,
            release,
        )
    }

    fn wait_until_released(&self) {
        let _ = self.entered.send(());
        let release = self
            .release
            .lock()
            .expect("group-commit test control lock poisoned")
            .take();
        if let Some(release) = release {
            crate::test_rendezvous::recv_within(
                &release,
                "the group-commit test control releasing the writer",
            );
        }
    }
}

#[cfg(test)]
struct ReleaseOnDrop(Option<std::sync::mpsc::SyncSender<()>>);

#[cfg(test)]
impl ReleaseOnDrop {
    fn new(sender: std::sync::mpsc::SyncSender<()>) -> Self {
        Self(Some(sender))
    }

    fn release(&mut self) -> Result<(), std::sync::mpsc::SendError<()>> {
        self.0
            .take()
            .expect("release guard must be used at most once")
            .send(())
    }
}

#[cfg(test)]
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[derive(Debug, Clone)]
pub struct RedbGroupCommitConfig {
    /// Max time to linger for more concurrent writers before committing a shallow
    /// barrier batch. `Duration::ZERO` disables lingering entirely (commit-on-drain
    /// = today's behavior, used as the bench baseline).
    pub linger: Duration,
    /// Only linger when the pending graph-mutation + Raft-entry count is BELOW
    /// this — a deep batch already coalesces well, so lingering buys nothing and
    /// just adds latency (adaptive).
    pub shallow_threshold: usize,
    /// Test-only gate used to hold the writer at the start of the linger window
    /// while the fixture queues the rest of its burst. Production opens never set
    /// this, so the live writer path has no synchronization hook.
    #[cfg(test)]
    pub(crate) test_control: Option<Arc<RedbGroupCommitTestControl>>,
}

impl RedbGroupCommitConfig {
    /// Resolve from env (Configuration discipline: read once at backend open).
    ///   * `EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US` — linger microseconds (default
    ///     `1000` = 1ms; `0` disables lingering / restores commit-on-drain).
    ///   * `EPISTEMIC_GRAPH_REDB_GROUP_SHALLOW` — shallow durable-work threshold
    ///     (default `32`); the writer lingers only while the pending graph-mutation
    ///     plus Raft-entry count is under it.
    pub fn from_env() -> Self {
        let linger_us = std::env::var("EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(1000);
        let shallow = std::env::var("EPISTEMIC_GRAPH_REDB_GROUP_SHALLOW")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or(32);
        Self {
            linger: Duration::from_micros(linger_us),
            // Never above the 4096 early-flush bound; at least 1.
            shallow_threshold: shallow.clamp(1, 4096),
            #[cfg(test)]
            test_control: None,
        }
    }
}

impl Default for RedbGroupCommitConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

/// Group-commit observability for the redb writer (CONCEPT:EG-KG.backend.adaptive-linger-coalesce), mirroring
/// `write_coalescer::BatchStats`. `ops / commits` is the average batch size = the
/// fsyncs-saved ratio; `lingered` counts commits that paid a micro-linger window.
/// `linger_waiting` exposes whether the writer is currently inside that bounded
/// receive window, which makes operational probes and deterministic contention
/// tests observe the mechanism instead of guessing from scheduler timing.
#[derive(Debug, Default)]
pub struct RedbCommitStats {
    /// Group-commit `WriteTransaction`s issued on the run-loop barrier/timeout path.
    pub commits: AtomicU64,
    /// Total graph ops folded across those commits.
    pub ops: AtomicU64,
    /// How many of those commits paid a micro-linger window.
    pub lingered: AtomicU64,
    /// True only while the writer is blocked in the bounded micro-linger receive.
    linger_waiting: AtomicBool,
}

impl RedbCommitStats {
    fn record(&self, ops: usize, lingered: bool) {
        self.commits.fetch_add(1, Ordering::Relaxed);
        self.ops.fetch_add(ops as u64, Ordering::Relaxed);
        if lingered {
            self.lingered.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn commits(&self) -> u64 {
        self.commits.load(Ordering::Relaxed)
    }
    pub fn ops(&self) -> u64 {
        self.ops.load(Ordering::Relaxed)
    }
    pub fn lingered(&self) -> u64 {
        self.lingered.load(Ordering::Relaxed)
    }
    /// Whether this writer is currently waiting for a concurrent mutation in its
    /// bounded micro-linger window.
    pub fn is_linger_waiting(&self) -> bool {
        self.linger_waiting.load(Ordering::Acquire)
    }
    /// Average group-commit batch size (`ops / commits`); 0.0 before any commit.
    pub fn avg_batch(&self) -> f64 {
        let c = self.commits();
        if c == 0 {
            0.0
        } else {
            self.ops() as f64 / c as f64
        }
    }
}

/// Fixed [`eg_types::MutationScopeIdentity`] for the admin-mutations / "cluster-admin"
/// coordinator store (`{persist_dir}/admin-mutations.redb`) — see [`AdminMutationStore`].
///
/// GRAPH-shaped, not native: every batch here is compiled with an "either"-family domain
/// (`ControlPlane`/`MultiGraph`), which `mutation_batch::finish_batch` maps to a
/// graph-shaped identity, so this identity must match that shape exactly or every
/// admin-saga commit fails closed with "mutation scope binding identity mismatch". Only
/// [`AdminOwner`]'s `OwnerLayout::LedgerOnly` can serve it: declaring no owner tables, it
/// is the one layout that places no shape requirement on the scopes it binds.
pub(crate) fn cluster_admin_scope_identity() -> Result<eg_types::MutationScopeIdentity, String> {
    eg_types::MutationScopeIdentity::fixed_graph(
        "native",
        "cluster-admin",
        crate::server::mutation_batch::COMPILED_BATCH_INCARNATION,
    )
}

/// [`eg_storage::PrivatePayloadIntegrity`] for the admin-mutations store, backed by the
/// SAME transaction-recovery-plan cipher (D-ORC-50) that sealed the bytes in the first
/// place (`handlers::txn::seal_txn_recovery_plan`/`open_txn_recovery_plan`). The storage
/// kernel enforces it on every private-payload read/write, so without a real authority
/// here every 2PC recovery-plan write/read fails closed — not optional plumbing.
#[cfg(feature = "security")]
struct TxnRecoveryPrivateIntegrity(crate::crypto::ValueCipher);

/// Authenticate one sealed private payload against the binding carried by its
/// parent receipt. SPARQL recovery binds the exact plaintext digest. Transaction
/// recovery binds the stable semantic intent so a retry may carry fresh OCC
/// observations without becoming a different operation.
#[cfg(feature = "security")]
pub(crate) fn authenticate_private_payload(
    cipher: &crate::crypto::ValueCipher,
    sealed: &[u8],
    expected_binding: &str,
) -> Result<(), String> {
    use sha2::Digest;
    let plaintext = cipher.unseal(sealed)?;
    let exact_binding = hex::encode(sha2::Sha256::digest(&plaintext));
    let semantic_binding = crate::server::txn::GraphTxnState::decode_recovery_plan(
        &plaintext,
        "private-recovery-integrity".to_string(),
    )
    .and_then(|txn| txn.replay_intent_digest())
    .ok();
    [Some(exact_binding), semantic_binding]
        .into_iter()
        .flatten()
        .any(|binding| binding == expected_binding)
        .then_some(())
        .ok_or_else(|| {
            "private recovery payload digest does not match its parent receipt".to_string()
        })
}

#[cfg(feature = "security")]
impl eg_storage::PrivatePayloadIntegrity for TxnRecoveryPrivateIntegrity {
    fn authenticate(&self, sealed: &[u8], expected_plaintext_digest: &str) -> Result<(), String> {
        authenticate_private_payload(&self.0, sealed, expected_plaintext_digest)
    }
}

/// Resolve the admin-mutations store's private-payload integrity authority. Reads the
/// SAME `EPISTEMIC_GRAPH_TXN_RECOVERY_KEY` (falling back to the shared data key) as every
/// [`ShardWriter`]'s own `txn_recovery_cipher`, but as a function of the environment
/// rather than of any one shard (none is constructed yet at this call site). `None` without the
/// `security` feature or the key: the write/read path already fails closed itself there.
#[cfg(feature = "security")]
pub(crate) fn admin_mutations_private_integrity(
) -> Option<Arc<dyn eg_storage::PrivatePayloadIntegrity>> {
    crate::crypto::ValueCipher::from_env_for_txn_recovery().map(|cipher| {
        Arc::new(TxnRecoveryPrivateIntegrity(cipher))
            as Arc<dyn eg_storage::PrivatePayloadIntegrity>
    })
}

#[cfg(not(feature = "security"))]
pub(crate) fn admin_mutations_private_integrity(
) -> Option<Arc<dyn eg_storage::PrivatePayloadIntegrity>> {
    None
}

/// Operator-facing identity of the ONE physical `admin-mutations.redb` owner file.
pub(crate) const ADMIN_MUTATIONS_STORE: &str = "epistemic-graph:admin-mutations";

/// The owner domain of `admin-mutations.redb`: it declares no owner tables of its own,
/// so `eg_storage` materializes only the ledger census for it.
pub(crate) type AdminOwner = eg_storage::LedgerOnlyOwner;
/// One kernel-issued scoped read over that store, as the consumers name it.
pub(crate) type AdminScopedRead<'a> = eg_storage::ScopedRead<'a, AdminOwner>;

/// The kernel-owned admin-mutations coordinator store (RF-RULING-004): the storage
/// kernel that owns the file, the one mutation kernel it issued, and the single bound
/// serving scope. It hands no database back out. Every row the file holds is the
/// ledger's own bookkeeping, so `create_owner` materializes the whole declared census
/// at creation and no "ensure the table exists" bootstrap of ours remains.
type AdminMutationStore = crate::sidecar_store::SidecarStore<AdminOwner>;

/// Open (creating if absent) the one admin-mutations owner file.
fn open_admin_mutations(path: &std::path::Path) -> Result<AdminMutationStore, String> {
    AdminMutationStore::open_with(
        path,
        ADMIN_MUTATIONS_STORE,
        cluster_admin_scope_identity()?,
        admin_mutations_private_integrity(),
        crate::store_authority::process_authority(),
    )
}

/// Handle to the redb write-through tier (CONCEPT:EG-KG.storage.kg-kg / EG-026). The dispatch
/// path holds an `Arc` of this and calls `record`/`record_durable`; each routes by
/// graph to one of K independent single-writer [`ShardWriter`]s, so K cores commit in
/// parallel. K=1 holds exactly one shard backed by canonical `graph-0.redb`.
pub struct RedbBackend {
    /// The K shards (len >= 1). Index `shard_index(graph_fname, K)` owns a graph.
    shards: Vec<ShardWriter>,
    /// Optional tenant catalog OVERRIDE for graph→shard routing (CONCEPT:EG-KG.sharding.empty-catalog-routing, M3).
    /// `None` (the default) ⇒ pure EG-026 FNV-1a routing, byte-for-byte unchanged. When
    /// `Some` AND it holds an explicit entry for a graph, that entry's shard wins
    /// (enabling rebalanceable / resharded placement); a graph with no entry STILL
    /// falls back to FNV-1a inside `TenantCatalog::resolve_shard`. So an empty catalog
    /// is indistinguishable from no catalog — the seam never destabilizes EG-026.
    catalog: Option<Arc<crate::server::persistence::tenant_catalog::TenantCatalog>>,
    /// Routing quiesce barrier for online resharding (CONCEPT:EG-KG.backend.catalog-shard-resolve). Catalog-attached
    /// durable writes resolve their shard + enqueue their op while holding a SHARED READ
    /// guard; [`RedbBackend::reshard_graph`] holds the EXCLUSIVE WRITE guard across a
    /// graph's move, so the route flip can never interleave a write (no lost / misrouted
    /// rows). When NO catalog is attached (the default) the write path never touches this
    /// — EG-026 is byte-for-byte unchanged.
    routing_epoch: Arc<RwLock<()>>,
    /// Local durable projection of cluster-wide/admin saga authority: in clustered
    /// serving this file is replayable consensus state on every group member rather than
    /// a pod-local coordinator, and single-node serving uses the same image directly.
    admin_mutations: AdminMutationStore,
    /// Durable cluster-topology self-report store (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 / W1.1).
    /// Always opened (like `admin_mutations` above) so the shape of `RedbBackend`
    /// doesn't vary with whether Raft happens to be configured; it is populated
    /// only when a clustered node self-reports at startup (`raft::node::start`).
    /// See `server::persistence::node_info_store` for the replication story.
    node_info: Arc<super::node_info_store::NodeInfoStore>,
    /// Durable cluster-hierarchy cache (CONCEPT:EG-KG.compute.leiden-hierarchy, VIZ-1). Own-file,
    /// like `node_info` above, but carries no replication story — a cache entry
    /// is node-local and always safely recomputable. See
    /// `server::persistence::cluster_hierarchy_store` for why membership never
    /// rides graph nodes/edges.
    cluster_hierarchy: Arc<super::cluster_hierarchy_store::ClusterHierarchyStore>,
}

/// Rebuild a live [`GraphCore`] from a durable [`GraphDump`] (CONCEPT:EG-KG.storage.100m-tenant —
/// tenant rehydration). Uses the SAME `add_node`/`add_edge`/semantic-restore path
/// `load_into` uses, so a rehydrated graph is byte-identical to a freshly loaded one.
/// The core is cleared first so a re-rehydrate is idempotent.
pub fn rehydrate_core_from_dump(core: &GraphCore, dump: &GraphDump) -> Result<(), String> {
    // Validate the binary-reconciled core+dynamic set before clearing the live
    // projection.  Reshard/hibernate rehydration therefore has the same atomic
    // engine-upgrade rule as ordinary startup.
    #[cfg(feature = "shacl")]
    crate::server::graph_schema::compose::validate_and_compose(&dump.schema_sources)?;
    core.clear();
    core.install_schema_sources(std::sync::Arc::clone(&dump.schema_sources));
    for (id, props) in &dump.nodes {
        core.add_node(id.clone(), props.clone());
    }
    for (src, tgt, props) in &dump.edges {
        let _ = core.add_edge(src.clone(), tgt.clone(), props.clone());
    }
    if !dump.semantic.is_empty() {
        if let Ok(store) = decode_durable_semantic(&dump.semantic) {
            *core.semantic_store.write() = store;
        }
    }
    Ok(())
}

pub(crate) fn seed_raw_row<'k, 'v, K, V>(
    shard_path: &std::path::Path,
    table: TableDefinition<K, V>,
    key: K::SelfType<'k>,
    value: V::SelfType<'v>,
) where
    K: redb::Key + 'static,
    V: redb::Value + 'static,
{
    let db = redb::Database::open(shard_path).expect("open shard for raw seed");
    let wtx = db.begin_write().expect("begin write");
    {
        let mut t = wtx.open_table(table).expect("open table for raw seed");
        t.insert(key, value).expect("insert raw seed row");
    }
    wtx.commit().expect("commit raw seed row");
}

#[cfg(test)]
mod tests {
    use super::*;
    // A raw `redb::Database` survives HERE and nowhere else: these fixtures plant
    // rows the production paths deliberately refuse to write, so they must reach
    // the file underneath the kernel's admission. Each opens the file only after
    // the backend that owns it shut down (redb's lock admits one holder).
    use redb::{Database, ReadableDatabase};

    use crate::mutation_batch::{
        DurabilityDomain, IncarnationId, LogicalName, MutationOperation, MutationOutboxIntent,
        MutationScopeIdentity, MutationSurface, ScopeTenantId, VersionExpectation,
        MUTATION_ACTOR_HEADER, MUTATION_BATCH_VERSION,
    };
    use crate::protocol::Request;
    use crate::server::auth::{
        build_shared_test_request, dispatch_test_on_heap as dispatch_on_heap,
    };
    #[cfg(feature = "tsdb")]
    use sha2::Digest;

    const TEST_AGENT: &str = "unit-test-agent";

    fn current_request(secret: &str, id: u64, graph: &str, method: Method) -> Request {
        build_shared_test_request(secret, id, graph, TEST_AGENT, method)
    }

    #[cfg(feature = "tsdb")]
    fn scoped_series_key(graph: &str, series: &str, principal: &str) -> String {
        let tenant_scope = crate::server::mutation_batch::opaque_coordinator_key(
            "carrier-tenant",
            "verified",
            "tenant-shared",
        );
        let actor_scope = format!(
            "principal:sha256:{}",
            hex::encode(sha2::Sha256::digest(principal.as_bytes()))
        );
        let owner_scope = crate::server::mutation_batch::opaque_coordinator_key(
            "carrier-owner",
            &tenant_scope,
            &actor_scope,
        );
        let graph_scope = crate::server::mutation_batch::opaque_coordinator_key(
            "timeseries-graph",
            &owner_scope,
            graph,
        );
        eg_tsdb::store::SeriesKey::new(tenant_scope, graph_scope, series).encode()
    }

    #[cfg(feature = "tsdb")]
    fn direct_test_series_key(graph: &str, series: &str) -> String {
        scoped_series_key(graph, series, &format!("principal:{TEST_AGENT}"))
    }

    #[cfg(feature = "tsdb")]
    fn envelope_test_series_key(graph: &str, series: &str) -> String {
        scoped_series_key(graph, series, TEST_AGENT)
    }

    fn props(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    fn caller_outbox_batch(graph: &str, batch_id: &str) -> MutationBatch {
        let identity = MutationScopeIdentity::graph(
            ScopeTenantId::new("caller-tenant").unwrap(),
            LogicalName::new(graph).unwrap(),
            IncarnationId::new("incarnation:test:redb-backend").unwrap(),
        );
        let actor = format!("principal:sha256:{}", "b".repeat(64));
        let mut batch = MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: batch_id.to_string(),
            envelope: crate::redb_store::fixture_operation_envelope(
                &identity, &actor, 17, batch_id,
            ),
            identity,
            placement_epoch: 1,
            version_expectation: VersionExpectation::Graph(0),
            fencing_token: Some(1),
            authoritative_state: None,
            operations: vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: Method::AddNode {
                    node_id: "backend-outbox-node".to_string(),
                    properties_msgpack: props(serde_json::json!({"source": "backend"})),
                },
            }],
            outbox: vec![MutationOutboxIntent {
                topic: "projection.backend".to_string(),
                key: batch_id.to_string(),
                payload: rmp_serde::to_vec_named(&serde_json::json!({"event": "backend"})).unwrap(),
                headers: std::collections::BTreeMap::from([(
                    MUTATION_ACTOR_HEADER.to_string(),
                    actor,
                )]),
            }],
            created_at_ms: 10,
        };
        let schema_digest = batch
            .envelope
            .operation()
            .expect("fixture carries an operation envelope")
            .method_schema_digest;
        batch.reseal_envelope(schema_digest).unwrap();
        batch
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn redb_backend_outbox_subscription_is_durable_and_topic_bound() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!(
            "eg-redb-outbox-subscribe-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let dir_s = dir.to_string_lossy().to_string();
        let graph = "backend-outbox-graph";
        let consumer = "backend-projection";
        let topic = "projection.backend";
        let batch = caller_outbox_batch(graph, "backend-outbox-batch");

        let backend = RedbBackend::open(dir_s.clone(), 64).expect("open redb backend");
        PersistenceBackend::commit_mutation_batch(&backend, graph, &batch, None, 11)
            .await
            .expect("caller batch commits through the backend");
        PersistenceBackend::subscribe_mutation_outbox(&backend, graph, consumer, topic)
            .await
            .expect("first subscription commits");
        PersistenceBackend::subscribe_mutation_outbox(&backend, graph, consumer, topic)
            .await
            .expect("same-topic subscription is idempotent");
        let conflict = PersistenceBackend::subscribe_mutation_outbox(
            &backend,
            graph,
            consumer,
            "projection.other",
        )
        .await
        .unwrap_err();
        assert!(conflict.contains("already subscribed to another topic"));
        backend.shutdown();
        drop(backend);

        let backend = RedbBackend::open(dir_s.clone(), 64).expect("reopen redb backend");
        PersistenceBackend::subscribe_mutation_outbox(&backend, graph, consumer, topic)
            .await
            .expect("subscription survives restart");
        let mut budget = OutboxClaimBudget::new(8, 100, 100).unwrap();
        let outcome =
            PersistenceBackend::claim_mutation_outbox(&backend, graph, consumer, &mut budget)
                .await
                .expect("subscribed backend claim succeeds");
        assert!(!outcome.claims.is_empty());
        backend.shutdown();
        let _ = std::fs::remove_dir_all(dir);
    }

    /// An implicit last-owner drop must synchronously join every shard writer.
    /// Reopening the same directory repeatedly catches the redb lock leak, while
    /// the per-open timeout catches a detached writer join without changing the
    /// process limit. The isolated xshard lifecycle child covers process-wide FD
    /// accumulation around an accepted Raft connection.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn implicit_drop_releases_writer_before_reopen() {
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = std::env::temp_dir().join(format!(
            "eg-redb-implicit-drop-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        let dir_s = dir.to_string_lossy().to_string();

        for _ in 0..8 {
            let open_dir = dir_s.clone();
            tokio::time::timeout(
                Duration::from_secs(5),
                tokio::task::spawn_blocking(move || {
                    let _backend = RedbBackend::open(open_dir, 64)
                        .expect("implicit drop must release the writer before reopen");
                }),
            )
            .await
            .expect("implicit writer drop must not hang")
            .expect("implicit writer-drop worker must join");
        }

        let _ = std::fs::remove_dir_all(dir);
    }

    /// Row count of ONE table in a shard file opened fresh, offline (WD5-BUG-04 —
    /// mirrors `shard_migrate.rs`'s own test helper of the same name).
    fn table_row_count<K, V>(path: &std::path::Path, def: redb::TableDefinition<K, V>) -> usize
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        let db = Database::open(path).expect("open shard for inspection");
        let rtx = db.begin_read().expect("begin read");
        match rtx.open_table(def) {
            Ok(t) => t.iter().expect("iterate table").count(),
            Err(_) => 0,
        }
    }

    /// Seed ONE valid, purge-safe `RESOURCE_RESERVATIONS` row (plus its matching
    /// `RESOURCE_RESERVATION_TENANT_INDEX` entry).
    ///
    /// WD5-BUG-05: unlike backup/restore and `migrate_shards` (pure byte copies),
    /// online reshard's PurgeGraph step (`purge_graph_rows` -> `clear_resource_rows`
    /// -> `check_resource_reservations_active`/`check_resource_tenant_index_consistency`)
    /// is PRE-EXISTING lifecycle-authority validation, unrelated to this lane's
    /// routing fix, that decodes every reservation row as a real
    /// `DurableResourceReservation` and requires a matching tenant-index entry
    /// before it will clear the source. A raw junk blob (`seed_raw_row`)
    /// fails that decode with "durable value is invalid" — not a fix defect, a
    /// seed-strategy mismatch. This builds a real, terminal/zero-held record (so
    /// `resource_reservation_row_is_active` is false) that the purge accepts.
    fn seed_valid_resource_reservation(
        shard_path: &std::path::Path,
        graph: &str,
        reservation_id: &str,
        tenant: &str,
    ) {
        use crate::epistemic_operations::{
            ResourceCapacitySnapshot, ResourceRequirement, ResourceReservationRecord,
            ResourceReservationRecordState, ResourceReservationRecordTargetKind,
            ResourceTargetSnapshot, ResourceTargetSnapshotKind,
        };

        // Field-for-field mirror of the private `redb_store::DurableResourceReservation`
        // wrapper (msgpack is structural, not nominal, so a same-shaped local type
        // encodes bytes the real type decodes) — see that struct's doc comment.
        #[derive(serde::Serialize)]
        struct DurableResourceReservationShadow {
            record: ResourceReservationRecord,
            held_cpu_weight: u64,
            held_memory_mib: u64,
            held_disk_mib: u64,
            held_process_slots: u64,
            fairness_debt: u64,
        }

        let record = ResourceReservationRecord {
            reservation_id: reservation_id.to_string(),
            tenant_ref: tenant.to_string(),
            owner_id: "owner:cx054".into(),
            work_item_id: "work:cx054".into(),
            fence: "fence:cx054".into(),
            attempt: 1,
            lease_epoch: 1,
            fencing_token: 1,
            input_fingerprint: "fingerprint:cx054".into(),
            host_ref: "host:cx054".into(),
            profile_name: "default".into(),
            profile_version: "1".into(),
            requirement: ResourceRequirement {
                cpu_weight: 0,
                memory_mib: 0,
                disk_mib: 0,
                process_slots: 0,
            },
            capacity_snapshot: ResourceCapacitySnapshot {
                cpu_weight: 0,
                memory_mib: 0,
                disk_mib: 0,
                process_slots: 0,
                host_revision: 0,
            },
            selected_target: ResourceTargetSnapshot {
                kind: ResourceTargetSnapshotKind::Local,
                alias: None,
                capability_labels: Vec::new(),
            },
            target_kind: ResourceReservationRecordTargetKind::Local,
            target_alias: None,
            repository_id: "repository:cx054".into(),
            branch: "branch:cx054".into(),
            concurrency_key: "concurrency:cx054".into(),
            concurrency_limit: None,
            repository_exclusive: false,
            branch_exclusive: false,
            required_labels: Vec::new(),
            anti_affinity: Vec::new(),
            fairness_group: "fairness:cx054".into(),
            fairness_cost: 0,
            disk_low_watermark_mib: None,
            disk_high_watermark_mib: None,
            disk_policy_key: "policy:cx054".into(),
            reserved_at_ms: 0,
            expires_at_ms: 0,
            expected_host_revision: None,
            expected_lifecycle_revision: None,
            // Terminal + zero held resources: `resource_reservation_row_is_active`
            // requires exactly this to let PurgeGraph clear the row.
            state: ResourceReservationRecordState::Released,
            revision: 1,
            lifecycle_revision: 1,
            tombstone: true,
        };
        let durable = DurableResourceReservationShadow {
            record,
            held_cpu_weight: 0,
            held_memory_mib: 0,
            held_disk_mib: 0,
            held_process_slots: 0,
            fairness_debt: 0,
        };
        let bytes = rmp_serde::to_vec_named(&durable).expect("encode reservation");

        let db = Database::open(shard_path).expect("open shard for raw seed");
        let wtx = db.begin_write().expect("begin write");
        {
            let mut reservations = wtx
                .open_table(crate::redb_store::RESOURCE_RESERVATIONS)
                .expect("open reservations table for raw seed");
            reservations
                .insert((graph, reservation_id), bytes.as_slice())
                .expect("insert reservation row");
            let mut tenant_index = wtx
                .open_table(crate::redb_store::RESOURCE_RESERVATION_TENANT_INDEX)
                .expect("open tenant index table for raw seed");
            tenant_index
                .insert((graph, tenant, reservation_id), reservation_id)
                .expect("insert tenant index row");
        }
        wtx.commit().expect("commit reservation seed");
    }

    /// Seed ONE valid, purge-safe `development_lane::HOLDS` row.
    ///
    /// WD5-BUG-05: `purge_graph_rows` -> `development_lane::clear_native_graph_rows_in_wtx`
    /// -> `drain_lane_holds` is the SAME pre-existing (not this lane's) lifecycle
    /// guard as the reservation case above: it decodes every hold as a real
    /// `DurableLaneHold` and fails closed unless the hold is drained (terminal
    /// state, no active charge, zero retained bytes). Field values below are the
    /// exact known-good shape `development_lane.rs`'s own `mod tests::hold()`
    /// helper uses (same `hold_id`/`input_fingerprint`/`base_sha` formats), just
    /// with `state`/`active_count_charged`/`retained_disk_bytes` set drained.
    fn seed_valid_development_lane_hold(
        shard_path: &std::path::Path,
        graph: &str,
        hold_id: &str,
        tenant: &str,
    ) {
        use crate::epistemic_operations::{
            DevelopmentLaneHold, DevelopmentLaneHoldHostTargetKind,
            DevelopmentLaneHoldSchemaVersion, DevelopmentLaneHoldState, DevelopmentLaneQuotaCharge,
            DevelopmentLaneQuotaChargeSchemaVersion,
        };

        // Field-for-field mirror of the private `development_lane::DurableLaneHold`
        // wrapper — see `seed_valid_resource_reservation`'s doc comment for why a
        // same-shaped local type is sufficient (msgpack is structural).
        #[derive(serde::Serialize)]
        struct DurableLaneHoldShadow {
            hold: DevelopmentLaneHold,
            observation_revision: u64,
            last_observed_at_ms: Option<u64>,
            terminal_state: Option<String>,
            terminal_expected_hold_revision: Option<u64>,
            cleanup_removal_proof_ref: Option<String>,
            cleanup_expected_hold_revision: Option<u64>,
            terminal_source_attempt: Option<u64>,
            terminal_source_lease_epoch: Option<u64>,
            terminal_source_fencing_token: Option<u64>,
            terminal_source_work_item_fence: Option<String>,
            resource_reservation_id: String,
            ttl_ms: u64,
        }

        let charge = DevelopmentLaneQuotaCharge {
            schema_version: DevelopmentLaneQuotaChargeSchemaVersion::V1,
            tenant_count: 0,
            owner_count: 0,
            session_count: 0,
            workspace_count: 0,
            repository_count: 0,
            host_count: 0,
            global_count: 0,
            tenant_predicted_disk_bytes: 0,
            owner_predicted_disk_bytes: 0,
            session_predicted_disk_bytes: 0,
            workspace_predicted_disk_bytes: 0,
            repository_predicted_disk_bytes: 0,
            host_predicted_disk_bytes: 0,
            global_predicted_disk_bytes: 0,
            tenant_observed_disk_bytes: 0,
            owner_observed_disk_bytes: 0,
            session_observed_disk_bytes: 0,
            workspace_observed_disk_bytes: 0,
            repository_observed_disk_bytes: 0,
            host_observed_disk_bytes: 0,
            global_observed_disk_bytes: 0,
            tenant_retained_disk_bytes: 0,
            owner_retained_disk_bytes: 0,
            session_retained_disk_bytes: 0,
            workspace_retained_disk_bytes: 0,
            repository_retained_disk_bytes: 0,
            host_retained_disk_bytes: 0,
            global_retained_disk_bytes: 0,
            revision: 0,
            policy_revision: 1,
        };

        let hold = DevelopmentLaneHold {
            schema_version: DevelopmentLaneHoldSchemaVersion::V1,
            hold_id: format!("v1:{}", "a".repeat(64)),
            lane_id: "lane:cx054".into(),
            tenant_ref: tenant.to_string(),
            request_id: "request:cx054".into(),
            work_item_id: "work:cx054".into(),
            owner_id: "owner:cx054".into(),
            session_id: "session:cx054".into(),
            fairness_group: "fairness:cx054".into(),
            workspace_ref: "workspace:cx054".into(),
            repository_id: "repository:cx054".into(),
            base_ref: "refs/heads/main".into(),
            base_sha: "a".repeat(40),
            branch: "branch:cx054".into(),
            worktree_locator: "lanes/cx054".into(),
            host_target_kind: DevelopmentLaneHoldHostTargetKind::Local,
            host_target_alias: None,
            host_ref: "host:cx054".into(),
            quota_policy_name: "default".into(),
            quota_policy_version: "1".into(),
            input_fingerprint: format!("v1:{}", "b".repeat(64)),
            predicted_disk_bytes: 0,
            observed_disk_bytes: 0,
            // Drained: zero retained bytes, no active charge, a terminal state
            // outside `drain_lane_holds`'s blocking set — exactly what lets
            // PurgeGraph clear the row.
            retained_disk_bytes: 0,
            active_count_charged: false,
            quota_charge: charge,
            state: DevelopmentLaneHoldState::Aborted,
            attempt: 1,
            lease_epoch: 1,
            fencing_token: 1,
            work_item_fence: "fence:cx054".into(),
            hold_revision: 1,
            lifecycle_revision: 1,
            allocation_revision: 1,
            cleanup_revision: 0,
            expires_at_ms: 10_000,
            last_renewed_at_ms: 1_000,
            cleanup_work_item_id: None,
            cleanup_work_item_fence: None,
            cleanup_attempt: None,
            cleanup_lease_epoch: None,
            cleanup_fencing_token: None,
            tombstone: true,
        };

        let durable = DurableLaneHoldShadow {
            hold,
            observation_revision: 0,
            last_observed_at_ms: None,
            terminal_state: None,
            terminal_expected_hold_revision: None,
            cleanup_removal_proof_ref: None,
            cleanup_expected_hold_revision: None,
            terminal_source_attempt: None,
            terminal_source_lease_epoch: None,
            terminal_source_fencing_token: None,
            terminal_source_work_item_fence: None,
            resource_reservation_id: "reservation:cx054".into(),
            ttl_ms: 1_000,
        };
        let bytes = rmp_serde::to_vec_named(&durable).expect("encode lane hold");

        let db = Database::open(shard_path).expect("open shard for raw seed");
        let wtx = db.begin_write().expect("begin write");
        {
            let mut holds = wtx
                .open_table(crate::redb_store::development_lane::HOLDS)
                .expect("open holds table for raw seed");
            holds
                .insert((graph, hold_id), bytes.as_slice())
                .expect("insert hold row");
        }
        wtx.commit().expect("commit hold seed");
    }

    /// A minimal `ServerState` (no persistence backend stored on it — the test
    /// drives the backend directly) with a persist dir set.
    fn new_state(persist_dir: Option<String>) -> Arc<RwLock<ServerState>> {
        let mut state = ServerState::new_for_test("test", ServerState::test_isolation(TEST_AGENT));
        state.persist_dir = persist_dir;
        Arc::new(RwLock::new(state))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn redb_durable_roundtrip() {
        // Held for the whole test: this test opens the backend TWICE (write side,
        // then a fresh reload side) and both opens must resolve the SAME
        // encryption-at-rest cipher or the reload's read fails with "encrypted
        // durable value is missing sealed framing" -- same requirement as
        // `k_gt_1_routes_to_deterministic_shard_and_survives_restart` above. Never
        // sets the key itself; only needs the ambient value to stay constant across
        // both opens. See `crate::crypto::acquire_test_env_lock`'s doc.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        // Commit nodes/edges through the awaited barrier, drop, and reload.
        let dir = std::env::temp_dir().join(format!("eg-redb-rt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        // ── write side ──
        let backend = RedbBackend::open(dir_s.clone(), 64).expect("open redb backend");
        backend
            .register_graph("__commons__", "__commons__", GraphType::Commons)
            .await
            .unwrap();
        backend
            .register_graph("g1", "g1", GraphType::Global)
            .await
            .unwrap();

        // The registry must have the graph for checkpoint to dump it; build a
        // minimal ServerState with the graph created + populated in memory.
        let state = new_state(Some(dir_s.clone()));
        {
            let mut s = state.write().await;
            let _ = s.registry.create_graph("g1", GraphType::Global, None);
        }
        // Apply mutations in memory AND record them through the backend. Fetch g1
        // by name — registry iteration order is not stable (HashMap), so
        // all_entries()[0] could be the pre-created `__commons__`.
        let core = {
            let s = state.read().await;
            s.registry.get("g1").map(|e| e.core.clone()).unwrap()
        };
        core.add_node(
            "a".into(),
            props(serde_json::json!({"type": "Task", "n": 1})),
        );
        core.add_node("b".into(), props(serde_json::json!({"type": "Task"})));
        let _ = core.add_edge("a".into(), "b".into(), props(serde_json::json!({"w": 2})));

        backend
            .record_durable(
                "g1",
                &Method::AddNode {
                    node_id: "a".into(),
                    properties_msgpack: props(serde_json::json!({"type": "Task", "n": 1})),
                },
            )
            .await
            .unwrap();
        backend
            .record_durable(
                "g1",
                &Method::AddNode {
                    node_id: "b".into(),
                    properties_msgpack: props(serde_json::json!({"type": "Task"})),
                },
            )
            .await
            .unwrap();
        backend
            .record_durable(
                "g1",
                &Method::AddEdge {
                    source_id: "a".into(),
                    target_id: "b".into(),
                    properties_msgpack: props(serde_json::json!({"w": 2})),
                },
            )
            .await
            .unwrap();
        backend.shutdown();
        drop(backend);

        // ── reload side: fresh backend + fresh empty state ──
        let backend2 = RedbBackend::open(dir_s.clone(), 64).expect("reopen redb backend");
        let state2 = new_state(Some(dir_s.clone()));
        let loaded = backend2.load_all(&state2).await.unwrap();
        assert_eq!(loaded, 2, "g1 + __commons__ reloaded from redb");

        let core2 = {
            let s = state2.read().await;
            s.registry
                .get("g1")
                .map(|e| e.core.clone())
                .expect("g1 reloaded")
        };
        assert_eq!(core2.node_count(), 2);
        assert_eq!(
            core2.get_node_properties("a"),
            Some(props(serde_json::json!({"type": "Task", "n": 1})))
        );
        assert_eq!(core2.get_edges().len(), 1);
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// GOC-16/NE-028: save/restore-on-drop for the encryption env vars, held under the SAME
    /// crate-wide [`crate::crypto::acquire_test_env_lock`] every other encryption-key
    /// test in this file and in `crypto.rs` contends on. Unlike
    /// `txn_commit_persists_to_redb`'s `std::sync::Once` (which only ever SETS
    /// `ENCRYPTION_KEY_ENV`, never unsets it — safe because no other test depends on
    /// it being absent), these tests specifically need the key ABSENT at least once,
    /// so a one-directional Once cannot be reused here; this restores the exact prior
    /// value (present or absent) on drop instead.
    #[cfg(feature = "security")]
    struct EncryptionRequiredEnvGuard {
        prev_key: Option<String>,
        prev_key_id: Option<String>,
        prev_key_version: Option<String>,
        prev_required: Option<String>,
    }

    #[cfg(feature = "security")]
    impl EncryptionRequiredEnvGuard {
        fn set(key: Option<&str>, required_mode: &str) -> Self {
            let prev_key = std::env::var(crate::crypto::ENCRYPTION_KEY_ENV).ok();
            let prev_key_id = std::env::var(crate::crypto::ENCRYPTION_KEY_ID_ENV).ok();
            let prev_key_version = std::env::var(crate::crypto::ENCRYPTION_KEY_VERSION_ENV).ok();
            let prev_required = std::env::var(crate::crypto::ENCRYPTION_REQUIRED_ENV).ok();
            match key {
                Some(k) => std::env::set_var(crate::crypto::ENCRYPTION_KEY_ENV, k),
                None => std::env::remove_var(crate::crypto::ENCRYPTION_KEY_ENV),
            }
            std::env::remove_var(crate::crypto::ENCRYPTION_KEY_ID_ENV);
            std::env::remove_var(crate::crypto::ENCRYPTION_KEY_VERSION_ENV);
            std::env::set_var(crate::crypto::ENCRYPTION_REQUIRED_ENV, required_mode);
            Self {
                prev_key,
                prev_key_id,
                prev_key_version,
                prev_required,
            }
        }

        fn set_with_ref(key: &str, key_id: &str, key_version: &str) -> Self {
            let guard = Self::set(Some(key), "warn");
            std::env::set_var(crate::crypto::ENCRYPTION_KEY_ID_ENV, key_id);
            std::env::set_var(crate::crypto::ENCRYPTION_KEY_VERSION_ENV, key_version);
            guard
        }
    }

    #[cfg(feature = "security")]
    impl Drop for EncryptionRequiredEnvGuard {
        fn drop(&mut self) {
            match self.prev_key.take() {
                Some(v) => std::env::set_var(crate::crypto::ENCRYPTION_KEY_ENV, v),
                None => std::env::remove_var(crate::crypto::ENCRYPTION_KEY_ENV),
            }
            match self.prev_key_id.take() {
                Some(v) => std::env::set_var(crate::crypto::ENCRYPTION_KEY_ID_ENV, v),
                None => std::env::remove_var(crate::crypto::ENCRYPTION_KEY_ID_ENV),
            }
            match self.prev_key_version.take() {
                Some(v) => std::env::set_var(crate::crypto::ENCRYPTION_KEY_VERSION_ENV, v),
                None => std::env::remove_var(crate::crypto::ENCRYPTION_KEY_VERSION_ENV),
            }
            match self.prev_required.take() {
                Some(v) => std::env::set_var(crate::crypto::ENCRYPTION_REQUIRED_ENV, v),
                None => std::env::remove_var(crate::crypto::ENCRYPTION_REQUIRED_ENV),
            }
        }
    }

    /// GOC-16 known-bad proof: `EPISTEMIC_GRAPH_ENCRYPTION_REQUIRED=on` with no
    /// `EPISTEMIC_GRAPH_ENCRYPTION_KEY` configured must refuse to open the durable
    /// store — before any writer thread spawns, before any listener binds — with a
    /// bounded, actionable diagnostic naming both env vars, never a silent open.
    #[cfg(feature = "security")]
    #[tokio::test(flavor = "multi_thread")]
    async fn encryption_required_on_refuses_to_open_without_a_key() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let _guard = EncryptionRequiredEnvGuard::set(None, "on");

        let dir = std::env::temp_dir().join(format!("eg-redb-enc-required-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let result = RedbBackend::open(dir_s.clone(), 64);

        // `unwrap_err()` would require `RedbBackend: Debug` (it formats the Ok
        // side on failure); the backend deliberately does not derive it, since
        // it owns live handles. Destructure instead of widening a public trait
        // bound just to satisfy a test.
        let message = match result {
            Ok(_) => panic!("opening with ENCRYPTION_REQUIRED=on and no key must fail closed"),
            Err(message) => message,
        };
        assert!(
            message.contains("REQUIRED") && message.contains("EPISTEMIC_GRAPH_ENCRYPTION_KEY"),
            "diagnostic must be bounded and name the missing key, got: {message}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// GOC-16: the mirror of the above — `ENCRYPTION_REQUIRED=on` with a key
    /// configured opens normally (a present key satisfies every mode identically).
    #[cfg(feature = "security")]
    #[tokio::test(flavor = "multi_thread")]
    async fn encryption_required_on_succeeds_when_a_key_is_set() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let _guard =
            EncryptionRequiredEnvGuard::set(Some("redb-encryption-required-test-key"), "on");

        let dir =
            std::env::temp_dir().join(format!("eg-redb-enc-required-ok-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let backend = RedbBackend::open(dir_s.clone(), 64)
            .expect("a configured key must open cleanly under ENCRYPTION_REQUIRED=on");
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// GOC-16 / BUG-248 known-bad proof: reopening an ALREADY-ENCRYPTED store with a
    /// DIFFERENT `EPISTEMIC_GRAPH_ENCRYPTION_KEY` than the one that sealed it must
    /// fail closed at open time — before any writer thread spawns or any listener
    /// binds — instead of opening successfully and only surfacing the mismatch later
    /// as a decrypt failure on whichever value a caller happens to read first.
    #[cfg(feature = "security")]
    #[tokio::test(flavor = "multi_thread")]
    async fn encryption_key_mismatch_refuses_to_reopen() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;

        let dir = std::env::temp_dir().join(format!("eg-redb-enc-mismatch-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        {
            let _guard = EncryptionRequiredEnvGuard::set(Some("original-key-material"), "warn");
            let backend = RedbBackend::open(dir_s.clone(), 64)
                .expect("first open with a fresh key must establish the canary and succeed");
            backend.shutdown();
        }

        let result = {
            let _guard =
                EncryptionRequiredEnvGuard::set(Some("a-completely-different-key"), "warn");
            RedbBackend::open(dir_s.clone(), 64)
        };
        let message = match result {
            Ok(_) => panic!("reopening with the WRONG key must fail closed, not open silently"),
            Err(message) => message,
        };
        assert!(
            message.contains(crate::crypto::ENCRYPTION_KEY_ENV)
                && message.contains("does not match"),
            "diagnostic must be bounded and name the mismatch, got: {message}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// NE-028: changing only the non-secret key reference is still a rotation
    /// boundary.  Even when the material happens to be unchanged, startup refuses
    /// the ambiguous configuration instead of silently moving the pin.
    #[cfg(feature = "security")]
    #[tokio::test(flavor = "multi_thread")]
    async fn encryption_key_reference_mismatch_refuses_to_reopen() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = std::env::temp_dir().join(format!(
            "eg-redb-enc-key-ref-mismatch-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        {
            let _guard =
                EncryptionRequiredEnvGuard::set_with_ref("stable-key-material", "kms/graph", "1");
            let backend = RedbBackend::open(dir_s.clone(), 64)
                .expect("first open must establish the pinned reference");
            backend.shutdown();
        }

        let result = {
            let _guard =
                EncryptionRequiredEnvGuard::set_with_ref("stable-key-material", "kms/graph", "2");
            RedbBackend::open(dir_s.clone(), 64)
        };
        let message = match result {
            Ok(_) => panic!("changing key version must fail closed before writer startup"),
            Err(message) => message,
        };
        assert!(message.contains("reference") && message.contains("does not match"));
        assert!(!message.contains("stable-key-material"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// GOC-16 / BUG-248: the mirror of the above — reopening with the SAME key
    /// verifies cleanly every time (the canary is checked, never rewritten, on a
    /// key that still matches).
    #[cfg(feature = "security")]
    #[tokio::test(flavor = "multi_thread")]
    async fn encryption_key_same_key_reopens_cleanly_across_multiple_opens() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let _guard = EncryptionRequiredEnvGuard::set(Some("stable-key-material"), "warn");

        let dir = std::env::temp_dir().join(format!("eg-redb-enc-stable-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        for _ in 0..3 {
            let backend = RedbBackend::open(dir_s.clone(), 64)
                .expect("reopening with the SAME key must keep succeeding");
            backend.shutdown();
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// GOC-16 / BUG-248: a store that has NEVER had a key configured (encryption
    /// stays off) opens exactly as before — no canary table write, no mismatch
    /// check, byte-for-byte the pre-existing plaintext behavior.
    #[cfg(feature = "security")]
    #[tokio::test(flavor = "multi_thread")]
    async fn encryption_never_configured_skips_the_canary_check_entirely() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let _guard = EncryptionRequiredEnvGuard::set(None, "off");

        let dir = std::env::temp_dir().join(format!("eg-redb-enc-none-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        for _ in 0..2 {
            let backend = RedbBackend::open(dir_s.clone(), 64)
                .expect("no key configured must keep opening exactly as before");
            backend.shutdown();
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// BUG-PE-055 known-bad proof, direction 1: a POPULATED PLAINTEXT store must not
    /// silently accept a brand-new key.
    ///
    /// Observed before this fix: the engine opened a previously-unencrypted store with
    /// a fresh key WITHOUT complaint, wrote a canary, and logged
    /// `redb encryption-at-rest ENABLED`. Every value already in the store then failed
    /// to unseal, one read at a time. This is the shape the deployment hits when the
    /// key lives under an ephemeral `AGENT_UTILITIES_DATA_DIR` (an emptyDir) while the
    /// store is on a persistent hostPath: a fresh key on every restart, against
    /// durable data.
    #[cfg(feature = "security")]
    #[tokio::test(flavor = "multi_thread")]
    async fn populated_plaintext_store_refuses_a_brand_new_key() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = std::env::temp_dir().join(format!(
            "eg-redb-enc-adopt-plaintext-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        // Write real durable rows with encryption OFF — a plaintext store.
        {
            let _guard = EncryptionRequiredEnvGuard::set(None, "off");
            let backend = RedbBackend::open(dir_s.clone(), 64).expect("plaintext open");
            backend
                .register_graph("plain", "plain", crate::protocol::GraphType::Global)
                .await
                .expect("register");
            backend.shutdown();
        }

        // Now hand it a key it has never seen.
        let result = {
            let _guard = EncryptionRequiredEnvGuard::set(Some("a-brand-new-key"), "warn");
            RedbBackend::open(dir_s.clone(), 64)
        };
        let message = match result {
            Ok(_) => panic!(
                "a populated PLAINTEXT store must refuse a brand-new key, not silently \
                 start encrypting over it"
            ),
            Err(message) => message,
        };
        assert!(
            message.contains(crate::crypto::ENCRYPTION_KEY_ENV)
                && message.contains("PLAINTEXT")
                && message.contains("destructive-read"),
            "the diagnostic must name the variable and the hazard, got: {message}"
        );
        assert!(
            !message.contains("a-brand-new-key"),
            "key material must never reach a diagnostic, got: {message}"
        );

        // The store is untouched: it still opens with encryption off.
        {
            let _guard = EncryptionRequiredEnvGuard::set(None, "off");
            let backend = RedbBackend::open(dir_s.clone(), 64)
                .expect("the refused open must leave the plaintext store serviceable");
            backend.shutdown();
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// BUG-PE-055 known-bad proof, direction 2: an ENCRYPTED store must not open with
    /// NO key at all.
    ///
    /// The no-key path never looked at the canary, so a store whose values are sealed
    /// opened cleanly and then failed one read at a time with
    /// `"encrypted durable value requires configured key material"`. That is the
    /// symmetric partner of the wrong-key refusal, and it holds regardless of
    /// `EPISTEMIC_GRAPH_ENCRYPTION_REQUIRED` — a missing key for an encrypted store is
    /// not a rollout-posture choice.
    #[cfg(feature = "security")]
    #[tokio::test(flavor = "multi_thread")]
    async fn encrypted_store_refuses_to_open_without_a_key() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-enc-nokey-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        {
            let _guard = EncryptionRequiredEnvGuard::set(Some("sealing-key-material"), "warn");
            let backend = RedbBackend::open(dir_s.clone(), 64)
                .expect("first open establishes the canary on an empty store");
            backend
                .register_graph("sealed", "sealed", crate::protocol::GraphType::Global)
                .await
                .expect("register");
            backend.shutdown();
        }

        // `off` deliberately: the refusal must not depend on the required-mode posture.
        let result = {
            let _guard = EncryptionRequiredEnvGuard::set(None, "off");
            RedbBackend::open(dir_s.clone(), 64)
        };
        let message = match result {
            Ok(_) => panic!("an ENCRYPTED store must refuse to open with no key configured"),
            Err(message) => message,
        };
        assert!(
            message.contains(crate::crypto::ENCRYPTION_KEY_ENV)
                && message.contains("encryption-at-rest metadata"),
            "the diagnostic must name the variable and the cause, got: {message}"
        );

        // With the original key it opens again — the refusal changed nothing on disk.
        {
            let _guard = EncryptionRequiredEnvGuard::set(Some("sealing-key-material"), "warn");
            let backend = RedbBackend::open(dir_s.clone(), 64)
                .expect("the original key must still open the store");
            backend.shutdown();
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// GOC-16: `off` and unset (`warn`, the shipped default) must NOT change
    /// today's behavior — a missing key still opens successfully, only the log
    /// output differs (proven at the unit level in `crypto.rs`, not observable
    /// through this `Result`).
    #[cfg(feature = "security")]
    #[tokio::test(flavor = "multi_thread")]
    async fn encryption_required_off_and_warn_still_open_without_a_key() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;

        for mode in ["off", "warn"] {
            let _guard = EncryptionRequiredEnvGuard::set(None, mode);
            let dir = std::env::temp_dir().join(format!(
                "eg-redb-enc-required-{mode}-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let dir_s = dir.to_string_lossy().to_string();

            let backend = RedbBackend::open(dir_s.clone(), 64)
                .unwrap_or_else(|e| panic!("mode {mode:?} must still open without a key: {e}"));
            backend.shutdown();
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// CONCEPT:EG-KG.storage.occ-durable-commit — a committed OCC transaction is durable through the redb
    /// backend: stage nodes/edge in a txn, commit through the full dispatch path
    /// (which commits its MutationBatch durably), drop, then
    /// reload via redb-only → the committed graph is recovered.
    #[tokio::test(flavor = "multi_thread")]
    async fn txn_commit_persists_to_redb() {
        use crate::protocol::ResultPayload;

        // Held for the whole test: the `Commit` below reaches `seal_txn_recovery_plan`
        // (fails closed without a configured `EPISTEMIC_GRAPH_ENCRYPTION_KEY`), and
        // this test ALSO reopens the backend ("reload via redb-only") — both opens
        // must resolve the same cipher. See `crate::crypto::acquire_test_env_lock`'s
        // doc for the full mechanism.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        // Under the write guard taken above, so it does not acquire anything itself.
        // ONE shared key value for the whole binary -- see
        // `crate::crypto::TEST_AT_REST_KEY`'s doc for why a per-module value was the
        // bug and not a convenience.
        #[cfg(feature = "security")]
        crate::crypto::provision_test_at_rest_key_under_write_guard();

        const SECRET: &str = "redb-txn-secret";
        let dir = std::env::temp_dir().join(format!("eg-redb-txn-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let backend: Arc<dyn crate::server::persistence::PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), 64).expect("open redb backend"));
        backend
            .register_graph("__commons__", "__commons__", GraphType::Commons)
            .await
            .unwrap();
        let state = new_state(Some(dir_s.clone()));
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            s.persistence = Some(backend.clone());
        }

        let req = |id: u64, method: Method| current_request(SECRET, id, "__commons__", method);
        let txn = match dispatch_on_heap(
            &state,
            req(
                1,
                Method::BeginTxn {
                    graph: None,
                    isolation: None,
                },
            ),
        )
        .await
        .result
        {
            Some(ResultPayload::String(t)) => t,
            other => panic!("BeginTxn id, got {other:?}"),
        };
        for (rid, nid) in [(2u64, "x"), (3, "y")] {
            let r = dispatch_on_heap(
                &state,
                req(
                    rid,
                    Method::TxnAddNode {
                        txn_id: txn.clone(),
                        node_id: nid.into(),
                        properties_msgpack: props(serde_json::json!({"type": "Task"})),
                        graph: None,
                    },
                ),
            )
            .await;
            assert!(matches!(r.result, Some(ResultPayload::Bool(true))));
        }
        let r = dispatch_on_heap(
            &state,
            req(
                4,
                Method::TxnAddEdge {
                    txn_id: txn.clone(),
                    source_id: "x".into(),
                    target_id: "y".into(),
                    properties_msgpack: props(serde_json::json!({})),
                    graph: None,
                },
            ),
        )
        .await;
        assert!(matches!(r.result, Some(ResultPayload::Bool(true))));

        let r = dispatch_on_heap(
            &state,
            req(
                5,
                Method::Commit {
                    txn_id: txn,
                    idempotency_key: None,
                },
            ),
        )
        .await;
        assert!(
            matches!(
                r.result,
                Some(ResultPayload::Json(serde_json::Value::Bool(true)))
            ),
            "commit ok: {:?}",
            r.error
        );

        backend.shutdown();
        // `shutdown()` stops the writer threads but does NOT close each shard's
        // `Database`; redb holds its advisory file lock for as long as any
        // `Arc<RedbBackend>` lives, so an in-process reopen of the same directory
        // fails with "Database already open. Cannot acquire lock." unless every
        // reference is actually released first.
        {
            let mut s = state.write().await;
            s.persistence = None;
        }
        drop(backend);

        let backend2 = RedbBackend::open(dir_s.clone(), 64).expect("reopen redb backend");
        let state2 = new_state(Some(dir_s.clone()));
        backend2.load_all(&state2).await.unwrap();
        let core2 = {
            let s = state2.read().await;
            s.registry
                .get("__commons__")
                .map(|e| e.core.clone())
                .expect("__commons__ reloaded")
        };
        assert!(
            core2.has_node("x") && core2.has_node("y"),
            "committed txn nodes durable in redb"
        );
        assert_eq!(
            core2.get_edges().len(),
            1,
            "committed txn edge durable in redb"
        );
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.tenant-delete-recreate-same — tenant DELETE + recreate-same-name must not drop the new
    /// graph's writes. Under redb-authoritative mode (read-through wired exactly as
    /// `main.rs` does it) we: create "g", add "n1"={v:1}, DELETE "g", recreate "g",
    /// add "n1"={v:2}, then read "n1" back through the full dispatch path. The
    /// recreated graph's node MUST read back as {v:2} — not the stale {v:1} left in
    /// redb by the first incarnation, not empty. Mirrors the agent-utilities
    /// `test_find_analogous_subgraphs` tenant-churn failure at the engine level.
    #[tokio::test(flavor = "multi_thread")]
    async fn delete_then_recreate_same_name_keeps_new_writes() {
        // Held for the whole test: opens the backend TWICE (initial + an in-process
        // reopen of the SAME dir, see the retry loop below) and both opens must
        // resolve the same encryption-at-rest cipher. See
        // `crate::crypto::acquire_test_env_lock`'s doc.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        use crate::protocol::ResultPayload;
        use crate::server::persistence::read_through::BackendReadThroughFactory;

        const SECRET: &str = "redb-recreate";
        let dir = std::env::temp_dir().join(format!("eg-redb-recreate-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let backend: Arc<dyn crate::server::persistence::PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), 256).expect("open"));
        let state = new_state(Some(dir_s.clone()));
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            s.persistence = Some(backend.clone());
            // Wire the durable read-through exactly like main.rs does under
            // authoritative mode — this is the read path that serves a RAM miss.
            let factory = Arc::new(BackendReadThroughFactory::new(backend.clone()));
            s.registry.set_read_through_factory(factory);
        }

        let req = |id: u64, method: Method| current_request(SECRET, id, "g", method);
        let create = |id: u64| {
            req(
                id,
                Method::CreateGraph {
                    graph_name: "g".into(),
                    graph_type: GraphType::Global,
                },
            )
        };
        let add = |id: u64, node: &str, v: i64| {
            req(
                id,
                Method::AddNode {
                    node_id: node.into(),
                    properties_msgpack: props(serde_json::json!({"v": v})),
                },
            )
        };
        let get = |id: u64, node: &str| {
            req(
                id,
                Method::GetNodeProperties {
                    node_id: node.into(),
                },
            )
        };

        // First incarnation: create + write n1={v:1} and stale={v:9}.
        assert!(dispatch_on_heap(&state, create(1)).await.error.is_none());
        assert!(dispatch_on_heap(&state, add(2, "n1", 1))
            .await
            .error
            .is_none());
        assert!(dispatch_on_heap(&state, add(3, "stale", 9))
            .await
            .error
            .is_none());

        // Delete the tenant.
        let del = dispatch_on_heap(
            &state,
            req(
                4,
                Method::DeleteGraph {
                    graph_name: "g".into(),
                },
            ),
        )
        .await;
        assert!(del.error.is_none(), "delete: {:?}", del.error);

        // Recreate SAME name. The new tenant writes ONLY n1={v:2}; it never writes
        // "stale" — that node belongs to the deleted incarnation and must be gone.
        let recreate = dispatch_on_heap(&state, create(5)).await;
        assert!(recreate.error.is_none(), "recreate: {:?}", recreate.error);
        assert!(dispatch_on_heap(&state, add(6, "n1", 2))
            .await
            .error
            .is_none());

        // (a) LIVE read-through: force every node out of RAM so the next read
        // RAM-MISSES and falls to the durable read-through (the eviction path is real
        // under authoritative mode — it bounds memory per CONCEPT:EG-KG.storage.read-through-seam-exercised).
        let ev = dispatch_on_heap(&state, req(7, Method::EvictLRU { max_nodes: 0 })).await;
        assert!(ev.error.is_none(), "evict: {:?}", ev.error);

        // The deleted incarnation's "stale" node must NOT resurrect from redb on a
        // RAM-miss read of the recreated graph.
        let r = dispatch_on_heap(&state, get(8, "stale")).await;
        let stale = match r.result {
            Some(ResultPayload::Raw(b)) => Some(b),
            Some(ResultPayload::Json(serde_json::Value::Null)) | None => None,
            other => panic!("unexpected get result: {other:?}"),
        };
        assert_eq!(
            stale, None,
            "deleted tenant's node 'stale' resurrected via read-through after recreate"
        );

        // And n1 reads back as the NEW write {v:2}.
        let r = dispatch_on_heap(&state, get(9, "n1")).await;
        let got = match r.result {
            Some(ResultPayload::Raw(b)) => Some(b),
            Some(ResultPayload::Json(serde_json::Value::Null)) | None => None,
            other => panic!("unexpected get result: {other:?}"),
        };
        assert_eq!(
            got,
            Some(props(serde_json::json!({"v": 2}))),
            "recreated tenant's node n1 must read back as the NEW write {{v:2}}, not stale/empty"
        );
        // `shutdown()` stops each shard's writer THREAD but does not close its
        // `Database` — the handle lives in the `ShardWriter`, so redb keeps its advisory
        // file lock for as long as ANY `Arc<RedbBackend>` survives (see the
        // identical note in `many_recreate_cycles_keep_inmemory_writes_visible`).
        // This test holds THREE: the local `backend`, the clone parked in
        // `state.persistence`, and the clone `BackendReadThroughFactory` wraps
        // inside `state.registry`'s installed read-through factory. `state` is not
        // read again after this point (part (b) below builds its own `state2`), so
        // dropping it wholesale — instead of clearing each field individually and
        // still risking a missed clone — is what actually releases the lock; the
        // sibling `many_recreate_cycles_keep_inmemory_writes_visible` gets away
        // with clearing only `persistence` because IT never installs a read-through
        // factory on `state` at all.
        backend.shutdown();
        drop(backend);
        drop(state);

        // The per-graph write-coalescer's background worker (spawned for "g"'s
        // SECOND incarnation by `add(6, ...)`, `write_coalescer::GraphWriter::spawn`)
        // also holds an `Arc<GraphCore>` — via its `read_through` field, an
        // `Arc<dyn PersistenceBackend>` clone — for as long as its own tokio task
        // is alive. Dropping `state` above synchronously drops the LAST `Sender`
        // into that worker's channel, but the worker task itself only notices the
        // channel closed (and drops its captured `core`) on its NEXT poll — an
        // async race `GraphWriter::spawn`'s own `JoinHandle` is discarded, so
        // nothing here can directly await. This is a test-only artifact of
        // reopening the SAME redb file IN-PROCESS (in production the OS releases
        // the file lock on process exit regardless — same rationale as the
        // `s.persistence = None` note on `dispatch_authoritative_durable_without_checkpoint`),
        // so bound it with a short retry rather than a flat sleep.
        let backend2: Arc<dyn crate::server::persistence::PersistenceBackend> = {
            let mut attempt = 0;
            loop {
                match RedbBackend::open(dir_s.clone(), 256) {
                    Ok(backend2) => break Arc::new(backend2),
                    Err(error) if attempt < 100 => {
                        attempt += 1;
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        let _ = error;
                    }
                    Err(error) => panic!("reopen: {error:?}"),
                }
            }
        };
        let state2 = new_state(Some(dir_s.clone()));
        backend2.load_all(&state2).await.unwrap();
        let core2 = {
            let s = state2.read().await;
            s.registry.get("g").map(|e| e.core.clone())
        };
        if let Some(core2) = core2 {
            assert!(
                !core2.has_node("stale"),
                "deleted tenant's node 'stale' resurrected from redb on load_all after recreate"
            );
            assert_eq!(
                core2.get_node_properties("n1"),
                Some(props(serde_json::json!({"v": 2}))),
                "recreated tenant's n1 must survive a reload as {{v:2}}"
            );
        }
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.many-repeated-create-delete — MANY repeated create→delete→recreate cycles on the SAME
    /// graph name must NOT enter a corrupted in-memory state that silently drops the
    /// recreated graph's writes (the vault-lane `__secrets__` failure). DISTINCT from
    /// KG-2.221 (durable redb purge): here every read is served HOT from RAM (no
    /// eviction, no reload) so the bug is purely the in-memory per-graph state keyed
    /// by name that DeleteGraph failed to reset — specifically the write-coalescer's
    /// cached `GraphWriter`, whose worker owns an `Arc<GraphCore>` of the DELETED
    /// incarnation. On recreate, `writer_for` returned the STALE writer (keyed by
    /// name) and routed the new tenant's writes into the orphaned core; the registry's
    /// fresh GraphCore stayed empty, so a hot RAM read saw nothing. The corruption
    /// accumulates: it appears the first cycle the coalescer batched a write.
    ///
    /// Tested for BOTH a plain name and a `__…__`-style reserved name (the report saw
    /// it on `__secrets__`). 50 cycles, each writing a DIFFERENT node so a stale read
    /// can't masquerade as a fresh one.
    #[tokio::test(flavor = "multi_thread")]
    async fn many_recreate_cycles_keep_inmemory_writes_visible() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::protocol::ResultPayload;
        use crate::server::persistence::read_through::BackendReadThroughFactory;

        const SECRET: &str = "redb-churn";
        const CYCLES: u64 = 50;

        for graph in ["g", "__secrets__"] {
            let dir = std::env::temp_dir().join(format!(
                "eg-redb-churn-{}-{}",
                std::process::id(),
                graph.trim_matches('_')
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let dir_s = dir.to_string_lossy().to_string();

            let backend: Arc<dyn crate::server::persistence::PersistenceBackend> =
                Arc::new(RedbBackend::open(dir_s.clone(), 256).expect("open"));
            let state = new_state(Some(dir_s.clone()));
            {
                let mut s = state.write().await;
                s.auth_secret = SECRET.to_string();
                s.persistence = Some(backend.clone());
                let factory = Arc::new(BackendReadThroughFactory::new(backend.clone()));
                s.registry.set_read_through_factory(factory);
            }

            let req = |id: u64, method: Method| current_request(SECRET, id, graph, method);

            let mut id = 0u64;
            let mut next = || {
                id += 1;
                id
            };

            for cycle in 0..CYCLES {
                // create
                let c = dispatch_on_heap(
                    &state,
                    req(
                        next(),
                        Method::CreateGraph {
                            graph_name: graph.into(),
                            graph_type: GraphType::Global,
                        },
                    ),
                )
                .await;
                assert!(c.error.is_none(), "cycle {cycle} create: {:?}", c.error);

                // write a node UNIQUE to this cycle
                let node = format!("n{cycle}");
                let a = dispatch_on_heap(
                    &state,
                    req(
                        next(),
                        Method::AddNode {
                            node_id: node.clone(),
                            properties_msgpack: props(serde_json::json!({"cycle": cycle})),
                        },
                    ),
                )
                .await;
                assert!(a.error.is_none(), "cycle {cycle} add: {:?}", a.error);

                // read it back HOT from RAM (no eviction) — the new tenant's write
                // MUST be visible. This is where a stale coalescer routes the write
                // into the deleted core and the fresh core reads back empty.
                let r = dispatch_on_heap(
                    &state,
                    req(
                        next(),
                        Method::GetNodeProperties {
                            node_id: node.clone(),
                        },
                    ),
                )
                .await;
                let got = match r.result {
                    Some(ResultPayload::Raw(b)) => Some(b),
                    Some(ResultPayload::Json(serde_json::Value::Null)) | None => None,
                    other => panic!("cycle {cycle} unexpected get result: {other:?}"),
                };
                assert_eq!(
                    got,
                    Some(props(serde_json::json!({"cycle": cycle}))),
                    "graph {graph:?} cycle {cycle}: recreated tenant's write to {node:?} \
                     was silently dropped (in-memory churn corruption)"
                );

                // IN-MEMORY proof: NodeCount reads the registry's live GraphCore
                // directly (NO durable read-through), so it sees ONLY what actually
                // landed in RAM. The recreated graph must hold EXACTLY this cycle's one
                // node. If the stale coalescer routed the write to the deleted core,
                // the live core is empty and this is 0 — the durable read-through above
                // would otherwise MASK the corruption by serving redb.
                let nc = dispatch_on_heap(&state, req(next(), Method::NodeCount)).await;
                let count = match nc.result {
                    Some(ResultPayload::Count(c)) => c,
                    other => panic!("cycle {cycle} unexpected node-count result: {other:?}"),
                };
                assert_eq!(
                    count, 1,
                    "graph {graph:?} cycle {cycle}: recreated tenant's live GraphCore must hold \
                     exactly the 1 node written this cycle (in-memory write was dropped)"
                );

                // delete (skip on the last cycle so we leave the graph live)
                if cycle + 1 < CYCLES {
                    let d = dispatch_on_heap(
                        &state,
                        req(
                            next(),
                            Method::DeleteGraph {
                                graph_name: graph.into(),
                            },
                        ),
                    )
                    .await;
                    assert!(d.error.is_none(), "cycle {cycle} delete: {:?}", d.error);
                }
            }

            backend.shutdown();
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// CONCEPT:EG-KG.backend.authoritative-dispatch — full dispatch path under AUTHORITATIVE mode: a write acked
    /// through `dispatch` is durable in redb WITHOUT any checkpoint, and reloads via
    /// redb `load_all` (the authoritative source). This proves the commit-before-ack
    /// barrier covers the real dispatch write path (incl. the coalescer) AND that the
    /// graph is recoverable under its real name with no checkpoint (graph_meta is
    /// durably registered on create + backfilled on write).
    #[tokio::test(flavor = "multi_thread")]
    async fn dispatch_authoritative_durable_without_checkpoint() {
        // Held for the whole test: opens the backend TWICE (initial dispatch-driven
        // writes, then a redb-only reload) and both opens must resolve the same
        // encryption-at-rest cipher. See `crate::crypto::acquire_test_env_lock`'s doc.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        use crate::protocol::ResultPayload;

        const SECRET: &str = "redb-auth-dispatch";
        let dir = std::env::temp_dir().join(format!("eg-redb-authd-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let backend: Arc<dyn crate::server::persistence::PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), 256).expect("open"));
        let state = new_state(Some(dir_s.clone()));
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            s.persistence = Some(backend.clone());
        }
        let req = |id: u64, method: Method| current_request(SECRET, id, "g_auth", method);
        // Create graph (durably registered) then write nodes — each dispatch returns
        // only after the durable commit.
        let r = dispatch_on_heap(
            &state,
            req(
                1,
                Method::CreateGraph {
                    graph_name: "g_auth".into(),
                    graph_type: GraphType::Global,
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "create: {:?}", r.error);
        for (rid, nid) in [(2u64, "a"), (3, "b"), (4, "c")] {
            let r = dispatch_on_heap(
                &state,
                req(
                    rid,
                    Method::AddNode {
                        node_id: nid.into(),
                        properties_msgpack: props(serde_json::json!({"id": nid})),
                    },
                ),
            )
            .await;
            assert!(r.error.is_none(), "addnode {nid}: {:?}", r.error);
            assert!(
                matches!(r.result, Some(ResultPayload::Bool(true)) | None) || r.error.is_none()
            );
        }

        // NO checkpoint. Drop the backend (flushes shutdown) and reload redb-only.
        //
        // `shutdown()` stops each shard's writer THREAD but does not close its
        // `Database` -- the handle lives in the `ShardWriter`, so redb keeps its advisory
        // file lock for as long as ANY `Arc<RedbBackend>` survives. This test holds
        // two: the local `backend` and the clone parked in `state.persistence`.
        // Reopening the same directory in-process while either is alive fails with
        // "Database already open. Cannot acquire lock." (In production the process
        // exits and the OS releases the lock, which is why only an in-process
        // reopen like this one is affected.) So actually drop both -- which is what
        // this comment always claimed was happening.
        backend.shutdown();
        {
            let mut s = state.write().await;
            s.persistence = None;
        }
        drop(backend);

        let backend2 = RedbBackend::open(dir_s.clone(), 256).expect("reopen");
        let state2 = new_state(Some(dir_s.clone()));
        let loaded = backend2.load_all(&state2).await.unwrap();
        assert!(loaded >= 1, "graphs recovered from redb without checkpoint");
        let core2 = {
            let s = state2.read().await;
            s.registry
                .get("g_auth")
                .map(|e| e.core.clone())
                .expect("g_auth recovered under real name")
        };
        assert!(core2.has_node("a") && core2.has_node("b") && core2.has_node("c"));
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.authoritative-dispatch — commit-before-ack: `record_durable` returns ONLY after the
    /// op is durably committed. The ONLY way the await completes is the group commit
    /// firing the waiter — there is no per-drained-batch commit path left for it to
    /// settle on incidentally. After the await returns, a SEPARATE reopened store
    /// sees the row — proving the await observed durable state, not just an enqueue.
    #[tokio::test(flavor = "multi_thread")]
    async fn record_durable_awaits_commit() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-durable-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open(dir_s.clone(), 64).expect("open");

        backend
            .record_durable(
                "g1",
                &Method::AddNode {
                    node_id: "a".into(),
                    properties_msgpack: props(serde_json::json!({"v": 1})),
                },
            )
            .await
            .expect("durable commit");

        // The await returned ⇒ the op is committed. Verify via a point read on the
        // SAME backend (goes through the owner thread, reflecting committed state).
        let got = backend.read_node("g1", "a").await.expect("read");
        assert_eq!(got, Some(props(serde_json::json!({"v": 1}))));
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.authoritative-dispatch — many concurrent `record_durable` calls COALESCE into group
    /// commits (NOT one fsync per op): all N complete, all N are durable. We can't
    /// directly count fsyncs here, but we assert all N awaited writers resolve Ok and
    /// every node is durably present — the coalescing path (one WriteTransaction per
    /// batch firing all the batch's waiters) is what makes that terminate quickly.
    #[tokio::test(flavor = "multi_thread")]
    async fn record_durable_coalesces_many_writers() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-coalesce-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = Arc::new(RedbBackend::open(dir_s.clone(), 256).expect("open"));

        let n = 200usize;
        let mut handles = Vec::new();
        for i in 0..n {
            let b = backend.clone();
            handles.push(tokio::spawn(async move {
                b.record_durable(
                    "g1",
                    &Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: props(serde_json::json!({"i": i})),
                    },
                )
                .await
            }));
        }
        for h in handles {
            h.await.unwrap().expect("each durable commit ok");
        }
        // Every node durable.
        for i in 0..n {
            let got = backend.read_node("g1", &format!("n{i}")).await.unwrap();
            assert!(got.is_some(), "n{i} durable");
        }
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── CONCEPT:EG-KG.storage.snapshot-read-off-writer — snapshot reads off the writer ───────────────────────────
    //
    // The point-read / read-through path (`read_node`) serves directly from a redb
    // `begin_read()` MVCC snapshot on the target shard's shared `Database`, routed by
    // the SAME EG-026 `shard_for`. It NEVER routes through the writer thread's channel
    // and NEVER forces a group-commit. These tests pin that invariant:
    //   (a) a read-through never increments any shard's commit counter (EG-026), and
    //       reads route to the correct shard under K>1 and return the right value;
    //   (b) reads complete CONCURRENTLY while many writes are in flight (MVCC, not
    //       serialized behind the writer queue);
    //   (c) a read after a write-ack sees the latest committed value (consistency).
    // None mutate env, so they need no LINGER_ENV_LOCK guard.

    /// (a) Read-through serves the node from a snapshot and triggers NO writer commit.
    /// Proven via the per-shard commit counters (EG-026 `commit_stats_all`): after the
    /// writes settle, a burst of reads leaves EVERY shard's commit count UNCHANGED.
    /// Uses K=3 so we also prove reads route to the correct shard (the value comes
    /// back) and that no OTHER shard commits either.
    #[tokio::test(flavor = "multi_thread")]
    async fn read_through_snapshot_triggers_no_writer_commit() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-snapread-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        // K=3 explicit (cfg(test) defaults to 1; open_with_shards honors the request on
        // a fresh dir). Graph names spread across shards via FNV-1a routing.
        let backend = RedbBackend::open_with_shards(dir_s.clone(), 64, 3).expect("open sharded");
        assert_eq!(backend.shard_count(), 3, "K=3 honored on a fresh dir");

        // Write one node into several graphs (spanning shards) + await each ack.
        let graphs = ["ga", "gb", "gc", "gd", "ge", "gf"];
        for (i, g) in graphs.iter().enumerate() {
            backend
                .record_durable(
                    g,
                    &Method::AddNode {
                        node_id: "n".into(),
                        properties_msgpack: props(serde_json::json!({ "g": g, "i": i })),
                    },
                )
                .await
                .expect("durable commit");
        }

        // Baseline: total commits across ALL shards, captured AFTER the writes settle.
        let baseline: u64 = backend.commit_stats_all().iter().map(|s| s.commits()).sum();

        // A burst of read-throughs on every graph (each routes to its owning shard via
        // `shard_for`, opens a `begin_read()` snapshot, returns the stored blob).
        for _ in 0..25 {
            for (i, g) in graphs.iter().enumerate() {
                let got = backend.read_node(g, "n").await.expect("snapshot read");
                assert_eq!(
                    got,
                    Some(props(serde_json::json!({ "g": g, "i": i }))),
                    "read routed to the correct shard for graph {g}"
                );
            }
        }
        // A genuinely absent node is still None (the snapshot read is not a fabricator).
        assert_eq!(
            backend.read_node("ga", "missing").await.expect("read"),
            None
        );

        // THE PROOF: not a single shard committed because of the reads.
        let after: u64 = backend.commit_stats_all().iter().map(|s| s.commits()).sum();
        assert_eq!(
            after, baseline,
            "reads must NOT route through the writer / force a commit \
             (commits {baseline} -> {after})"
        );
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// (b) Reads succeed CONCURRENTLY while writes are in flight — the MVCC snapshot
    /// read does not serialize behind the durable write path. We fan a large burst of
    /// `record_durable` writes and, at the same time, fan a burst of reads of an
    /// already-committed seed node; every read resolves Ok and sees the seed value
    /// even while the writer is saturated with commits.
    #[tokio::test(flavor = "multi_thread")]
    async fn reads_run_concurrently_with_inflight_writes() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-snapconc-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = Arc::new(RedbBackend::open(dir_s.clone(), 256).expect("open"));

        // Seed a committed node the readers will keep seeing.
        let seed = props(serde_json::json!({ "seed": true }));
        backend
            .record_durable(
                "g1",
                &Method::AddNode {
                    node_id: "seed".into(),
                    properties_msgpack: seed.clone(),
                },
            )
            .await
            .expect("seed commit");

        let mut tasks = Vec::new();
        // Writers: many concurrent durable writes keep the single writer busy.
        for i in 0..200usize {
            let b = backend.clone();
            tasks.push(tokio::spawn(async move {
                b.record_durable(
                    "g1",
                    &Method::AddNode {
                        node_id: format!("w{i}"),
                        properties_msgpack: props(serde_json::json!({ "i": i })),
                    },
                )
                .await
                .map(|_| None)
            }));
        }
        // Readers: concurrently snapshot-read the seed; must not block on the writer.
        for _ in 0..200usize {
            let b = backend.clone();
            let want = seed.clone();
            tasks.push(tokio::spawn(async move {
                let got = b.read_node("g1", "seed").await?;
                assert_eq!(got, Some(want), "concurrent read sees the committed seed");
                Ok::<Option<()>, String>(Some(()))
            }));
        }
        let mut reads_ok = 0usize;
        for t in tasks {
            if let Some(()) = t.await.unwrap().expect("read/write task ok") {
                reads_ok += 1;
            }
        }
        assert_eq!(reads_ok, 200, "every concurrent reader completed");
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// (c) A read after a write-ack sees the written value, and after an UPDATE-ack the
    /// snapshot reflects the NEW value — i.e. reads see the latest COMMITTED state per
    /// shard (commit-before-ack ⇒ an acked write is on disk ⇒ a fresh `begin_read`
    /// after the ack sees it).
    #[tokio::test(flavor = "multi_thread")]
    async fn read_after_ack_sees_latest_committed() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-snapack-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open(dir_s.clone(), 64).expect("open");

        backend
            .record_durable(
                "g1",
                &Method::AddNode {
                    node_id: "a".into(),
                    properties_msgpack: props(serde_json::json!({ "v": 1 })),
                },
            )
            .await
            .expect("commit v1");
        assert_eq!(
            backend.read_node("g1", "a").await.expect("read v1"),
            Some(props(serde_json::json!({ "v": 1 }))),
            "snapshot opened after the ack sees the committed write"
        );

        // Overwrite the same node; after the ack the snapshot reflects the new value.
        backend
            .record_durable(
                "g1",
                &Method::AddNode {
                    node_id: "a".into(),
                    properties_msgpack: props(serde_json::json!({ "v": 2 })),
                },
            )
            .await
            .expect("commit v2");
        assert_eq!(
            backend.read_node("g1", "a").await.expect("read v2"),
            Some(props(serde_json::json!({ "v": 2 }))),
            "a fresh snapshot after the update-ack sees the LATEST committed value"
        );
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.adaptive-linger-coalesce — the adaptive micro-linger COALESCES concurrent in-flight
    /// authoritative writers into fewer, larger group commits WITHOUT losing
    /// durability. We enqueue N ops directly onto the shard's writer channel (see the
    /// GOC-70 rule 3 note below) and assert:
    ///   * every awaited writer resolves Ok and every node is durably present
    ///     (durability guarantee unchanged — commit-before-ack still holds),
    ///   * `ops` accounts for every op, exactly (no double count, no drop),
    ///   * the average batch size (`ops / commits`) climbs well above 1, i.e. the
    ///     linger folded many writers into one fsync (the profiled win),
    ///   * lingered commits were actually exercised.
    ///
    /// The live cadence is the only one now (the `Each` per-drained-batch policy is
    /// deleted), and pre-EG-024 a drained channel committed immediately at ~1
    /// op/fsync — which is exactly the shape the linger exists to widen.
    ///
    /// GOC-70 (fix/micro-linger-stats-race): on the real 2-vCPU CI runner this test
    /// once failed with `ops == 0` — NOT a "stats read before the writer ran" race
    /// (the durability assertions right above prove every write was already durably
    /// on disk by then, and `stats.record()` is called on the SAME writer thread
    /// strictly before that batch's `done` oneshots fire, so anything already acked
    /// is, by program order + the oneshot channel's release/acquire handoff, already
    /// reflected in the counters). The real bug: `handle_cmd`'s early-flush bound
    /// (`pending.ops.len() >= flush_threshold`, memory-safety valve for a burst that
    /// outpaces the tick) called `commit_and_notify` DIRECTLY, bypassing
    /// `stats.record()` entirely — a batch that settles via that path durably lands
    /// (acks fire correctly) but was silently never accounted for. `n` (256) here
    /// used to exactly equal the old capacity-512-derived `flush_threshold` (256),
    /// so whenever every op landed in the channel before the writer's first drain —
    /// routine when a couple of tokio workers contend for 2 real CPUs, rare when the
    /// writer OS thread gets its own core almost immediately on a many-core dev
    /// box — the WHOLE batch took that unaccounted path. Fixed at the root
    /// (`handle_cmd` now records stats on every commit path it takes, not only the
    /// run-loop's own drain/linger/tick path — see the `flush` helper in
    /// `handle_cmd`) so `ops`/`commits` are accounted identically no matter which
    /// path a batch settles through, on any core count (GOC-70 rule 1). The capacity
    /// below is also raised so `flush_threshold` sits well above `n`, keeping this
    /// test's OWN outcome about the micro-linger mechanism specifically, not
    /// incidentally about the separate early-flush bound.
    ///
    /// GOC-70 rule 3 (deterministic contention, not scheduler luck): the original
    /// shape fanned N `record_durable` calls out across N separate `tokio::spawn`
    /// tasks and relied on the executor overlapping enough of them within a short
    /// linger window — exactly the anti-pattern this file's own
    /// `write_coalescer::tests::concurrent_writes_coalesce_into_fewer_lock_
    /// acquisitions` was rewritten away from after it broke
    /// `dispatch_coalesces_concurrent_writes_to_one_graph` in 2.25.0 (true on a
    /// lightly-loaded many-core host; not guaranteed when a couple of tokio workers
    /// and a `spawn_blocking` hop each contend for 2 real CPUs). A test-only gate
    /// pauses the writer immediately before it starts the linger receive, signals
    /// the fixture, and is released only after the remaining 255 commands are
    /// queued. The burst therefore lands in the same pending batch by construction;
    /// no scheduling overlap or wall-clock window is part of the assertion.
    ///
    /// Serializes the remaining env-mutating linger fixture. The coalescing fixture
    /// injects its config directly; the disabled baseline still reads
    /// `EPISTEMIC_GRAPH_REDB_GROUP_*` once at open.
    static LINGER_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[tokio::test(flavor = "multi_thread")]
    async fn micro_linger_coalesces_concurrent_writers() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-linger-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let (control, entered_rx, release_tx) = RedbGroupCommitTestControl::new();
        let backend = Arc::new(
            RedbBackend::open_with_group_commit_config(
                dir_s.clone(),
                // Channel capacity 4096 ⇒ `resolve_flush_threshold` (capacity/2, clamped
                // 256..16384) resolves to 2048 — comfortably above `n` below (256), so
                // the writer's early-flush memory bound cannot fire for this batch. This
                // keeps the fixture focused on the micro-linger mechanism rather than
                // the separate early-flush bound.
                4096,
                RedbGroupCommitConfig {
                    linger: Duration::from_millis(2),
                    shallow_threshold: 256,
                    test_control: Some(control),
                },
            )
            .expect("open"),
        );
        // Declare this after `backend`: if an assertion unwinds while the writer
        // is held at the injected gate, the guard releases it before the backend's
        // implicit Drop tries to join that writer.
        let mut release = ReleaseOnDrop::new(release_tx);

        let n = 256usize;
        // GOC-70 rule 3: queue the first command, then wait for the writer's
        // injected pre-linger signal before queueing the rest. The writer is held
        // until this loop has filled the same channel batch, so this remains
        // deterministic even when the test binary is CPU-starved.
        let shard = backend.shard_for("g1");
        // Observe the writer that actually owns this graph. `commit_stats()` is the
        // shard-0 compatibility view and is not a valid oracle when auto-sizing K>1.
        let stats = shard.stats.clone();
        let mut receivers = Vec::with_capacity(n);
        let enqueue = |i: usize| {
            let (done, rx) = oneshot::channel();
            shard
                .tx
                .send(Cmd::Mutation {
                    graph: "g1".to_string(),
                    method: Box::new(Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: props(serde_json::json!({"i": i})),
                    }),
                    done,
                })
                .expect("redb writer thread alive");
            rx
        };
        receivers.push(enqueue(0));
        tokio::task::spawn_blocking(move || {
            crate::test_rendezvous::recv_within(
                &entered_rx,
                "the redb writer entering the injected linger gate",
            );
        })
        .await
        .expect("linger gate waiter must complete");
        for i in 1..n {
            receivers.push(enqueue(i));
        }
        release
            .release()
            .expect("redb writer must still be held at the linger gate");
        for rx in receivers {
            rx.await
                .expect("redb writer dropped completion")
                .expect("each durable commit ok");
        }
        // Durability: every node present.
        for i in 0..n {
            assert!(
                backend
                    .read_node("g1", &format!("n{i}"))
                    .await
                    .unwrap()
                    .is_some(),
                "n{i} durable"
            );
        }
        // The win: both writers folded into one commit rather than one commit each.
        let commits = stats.commits();
        let ops = stats.ops();
        assert_eq!(ops, n as u64, "every op must be accounted for exactly once");
        assert!(
            commits < n as u64,
            "linger must coalesce: {commits} commits for {ops} ops (expected << {n})"
        );
        assert!(
            stats.avg_batch() > 1.5,
            "avg batch {:.2} should be well above the 1-op/fsync baseline",
            stats.avg_batch()
        );
        assert!(stats.lingered() > 0, "micro-linger path was exercised");
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.adaptive-linger-coalesce — with the linger DISABLED (`LINGER_US=0`) the writer falls back
    /// to the exact commit-on-drain behavior, and durability is identical. This pins
    /// the baseline the bench measures against and proves the knob is a real opt-out.
    #[tokio::test(flavor = "multi_thread")]
    async fn micro_linger_disabled_preserves_durability() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-nolinger-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = {
            // Serialized vs the coalesce test so its linger value can't leak into our `open`.
            let _env = LINGER_ENV_LOCK.lock().unwrap();
            std::env::set_var("EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US", "0");
            let b = Arc::new(RedbBackend::open(dir_s.clone(), 256).expect("open"));
            std::env::remove_var("EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US");
            b
        };

        let stats = backend.commit_stats();
        let n = 64usize;
        let mut handles = Vec::new();
        for i in 0..n {
            let b = backend.clone();
            handles.push(tokio::spawn(async move {
                b.record_durable(
                    "g1",
                    &Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: props(serde_json::json!({"i": i})),
                    },
                )
                .await
            }));
        }
        for h in handles {
            h.await.unwrap().expect("each durable commit ok");
        }
        for i in 0..n {
            assert!(
                backend
                    .read_node("g1", &format!("n{i}"))
                    .await
                    .unwrap()
                    .is_some(),
                "n{i} durable"
            );
        }
        // No commit ever lingered when the knob is 0.
        assert_eq!(
            stats.lingered(),
            0,
            "linger disabled ⇒ zero lingered commits"
        );
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── CONCEPT:EG-KG.storage.read-through-seam-exercised — read-through-on-RAM-miss + safe authoritative eviction ──

    /// Populate `core` AND redb with `n` durable nodes, then install the read-through
    /// factory + the backend on `state` exactly as `main.rs` does under authoritative
    /// mode. Returns the live core for "g1".
    async fn seed_authoritative(
        backend: &Arc<dyn PersistenceBackend>,
        state: &Arc<RwLock<ServerState>>,
        n: usize,
    ) -> Arc<GraphCore> {
        {
            let mut s = state.write().await;
            s.persistence = Some(backend.clone());
            let _ = s.registry.create_graph("g1", GraphType::Global, None);
            // Wire read-through exactly like startup (attaches to g1 + __commons__).
            let factory = Arc::new(
                crate::server::persistence::read_through::BackendReadThroughFactory::new(
                    backend.clone(),
                ),
            );
            s.registry.set_read_through_factory(factory);
        }
        let core = {
            let s = state.read().await;
            s.registry.get("g1").map(|e| e.core.clone()).unwrap()
        };
        for i in 0..n {
            let p = props(serde_json::json!({"type": "Task", "i": i}));
            // RAM
            core.add_node(format!("n{i}"), p.clone());
            // Durable (commit-before-ack) — every node is provably on disk.
            backend
                .record_durable(
                    "g1",
                    &Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: p,
                    },
                )
                .await
                .expect("durable commit");
        }
        core
    }

    /// (a) Memory bounded: filling past the cap under authoritative mode + read-through
    /// EVICTS down to the cap — RAM resident count is bounded, eviction actually ran.
    /// (b)/(c) An EVICTED node still reads back its correct properties via the
    /// read-through (it is not in RAM, but redb serves it) — no loss across the boundary.
    #[tokio::test(flavor = "multi_thread")]
    async fn authoritative_eviction_bounds_memory_and_reads_through() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-evict-rt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), 256).expect("open"));
        let state = new_state(Some(dir_s.clone()));

        let n = 50usize;
        let cap = 10usize;
        let core = seed_authoritative(&backend, &state, n).await;
        assert_eq!(
            core.node_count(),
            n,
            "all {n} nodes resident before eviction"
        );

        // Evict down to the cap (the per-graph max-nodes backstop).
        let evicted = crate::persist::evict_oversized_all(&state, cap).await;
        assert_eq!(evicted, n - cap, "evicted everything above the cap");

        // (a) memory bounded: RAM resident count is at the cap, NOT n.
        assert_eq!(
            core.node_count(),
            cap,
            "RAM resident count bounded to the cap after eviction"
        );

        // (b)/(c) the EVICTED nodes (lowest indices n0..) are gone from RAM yet read
        // back their exact properties via read-through from redb — no data loss.
        assert!(!core.has_node("n0"), "n0 evicted from RAM topology");
        for i in 0..(n - cap) {
            let got = core.get_node_properties(&format!("n{i}"));
            assert_eq!(
                got,
                Some(props(serde_json::json!({"type": "Task", "i": i}))),
                "evicted node n{i} reads back correct properties via read-through"
            );
        }
        // A node still resident reads from RAM as before.
        for i in (n - cap)..n {
            assert!(core.has_node(&format!("n{i}")), "n{i} still resident");
            assert_eq!(
                core.get_node_properties(&format!("n{i}")),
                Some(props(serde_json::json!({"type": "Task", "i": i})))
            );
        }
        // A genuinely absent node is still None (read-through is not a fabricator).
        assert_eq!(core.get_node_properties("does-not-exist"), None);

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// No data loss: a node NOT durably in redb is NEVER evicted, even if it is the
    /// LRU candidate. We add an extra RAM-only node with the LOWEST index (so it is
    /// first in the LRU order) but do NOT record it durably; eviction must keep it
    /// resident (durability unconfirmed) and instead evict only durable nodes.
    #[tokio::test(flavor = "multi_thread")]
    async fn authoritative_eviction_never_drops_undurable_node() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-evict-safe-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), 64).expect("open"));
        let state = new_state(Some(dir_s.clone()));

        // Insert the un-durable node FIRST so it has the lowest NodeIndex (front of
        // the LRU order — the one the cache would normally drop first).
        let core = {
            let mut s = state.write().await;
            s.persistence = Some(backend.clone());
            let _ = s.registry.create_graph("g1", GraphType::Global, None);
            let factory = Arc::new(
                crate::server::persistence::read_through::BackendReadThroughFactory::new(
                    backend.clone(),
                ),
            );
            s.registry.set_read_through_factory(factory);
            s.registry.get("g1").map(|e| e.core.clone()).unwrap()
        };
        core.add_node(
            "ghost".into(),
            props(serde_json::json!({"type": "Task", "durable": false})),
        );
        // Now 10 durable nodes.
        for i in 0..10usize {
            let p = props(serde_json::json!({"type": "Task", "i": i}));
            core.add_node(format!("n{i}"), p.clone());
            backend
                .record_durable(
                    "g1",
                    &Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: p,
                    },
                )
                .await
                .expect("durable commit");
        }
        assert_eq!(core.node_count(), 11);

        // Cap = 5 ⇒ 6 candidates: ghost (lowest index) + n0..n4. Only the 5 durable
        // ones may be dropped; ghost is kept (its durability cannot be confirmed).
        let evicted = crate::persist::evict_oversized_all(&state, 5).await;
        assert_eq!(
            evicted, 5,
            "only the 5 confirmed-durable candidates evicted"
        );
        assert!(
            core.has_node("ghost"),
            "un-durable node kept resident — never evicted (no data loss)"
        );
        // The un-durable node has no redb row, so a (hypothetical) miss would not
        // resurrect it — which is exactly why eviction must not drop it. Confirm it
        // still reads from RAM.
        assert_eq!(
            core.get_node_properties("ghost"),
            Some(props(serde_json::json!({"type": "Task", "durable": false})))
        );

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.storage.one-fsync-covers-raft — ONE fsync covers a Raft log entry AND its graph mutation.
    /// The only way an awaited op completes is the group commit firing. We launch a
    /// `record_durable` (M2 graph mutation) and a
    /// `raft_log_append` (Raft log entry) CONCURRENTLY into the same tick window;
    /// both share ONE `Pending` batch → ONE `WriteTransaction` → ONE fsync. We then
    /// prove BOTH landed durably (the graph row AND the log row).
    #[cfg(feature = "raft")]
    #[tokio::test(flavor = "multi_thread")]
    async fn raft_log_and_mutation_share_one_group_commit() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-1txn-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = Arc::new(RedbBackend::open(dir_s.clone(), 64).expect("open"));

        // Fire both into the SAME group-commit window, concurrently. With Interval
        // fsync, neither completes until the group commit fires — so if they both
        // resolve from ONE flush, they rode ONE transaction together.
        let b1 = backend.clone();
        let mutation = tokio::spawn(async move {
            b1.record_durable(
                "g1",
                &Method::AddNode {
                    node_id: "shared".into(),
                    properties_msgpack: props(serde_json::json!({"v": 7})),
                },
            )
            .await
        });
        let b2 = backend.clone();
        let log_blob = rmp_serde::to_vec_named(&serde_json::json!({"entry": 1})).unwrap();
        let log = tokio::spawn(async move { b2.raft_log_append(0, vec![(1u64, log_blob)]).await });

        mutation.await.unwrap().expect("graph mutation durable");
        log.await.unwrap().expect("raft log append durable");

        // Both are on disk: the graph row AND the log row at (group 0, index 1).
        let node = backend.read_node("g1", "shared").await.expect("read node");
        assert_eq!(node, Some(props(serde_json::json!({"v": 7}))));
        let entries = backend.raft_log_read(0, 1, 1).expect("read log");
        assert_eq!(
            entries.len(),
            1,
            "the log entry committed in the same flush"
        );
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Cross-modal ACID (CONCEPT:EG-KG.txn.reader-never-sees-node) ─────────────────────────────────────

    use crate::protocol::{Response, ResultPayload};
    use crate::server::auth::VerifiedRequestContext;
    use crate::server::handlers::txn::try_handle as txn_try_handle;

    fn cm_dir(tag: &str) -> String {
        // The cross-modal handler-commit path seals its transaction recovery plan
        // (`server::handlers::txn::seal_txn_recovery_plan`), which fail-closed REQUIRES
        // `EPISTEMIC_GRAPH_ENCRYPTION_KEY` to be configured — the same seal requirement
        // the xshard harness hit. Provision it ONCE before any backend opens. Encryption
        // is symmetric and transparent to every durable round-trip these tests make, so
        // a keyed store behaves identically for their assertions. The env var is
        // process-global, and it is now ONE shared value for the whole binary (see
        // `crate::crypto::TEST_AT_REST_KEY`'s doc -- a per-module value is what made
        // the ambient key change up to seven times over one run). Every caller of
        // `cm_dir` holds `crate::crypto::acquire_test_env_lock()` for its entire test
        // body (see each call site), so the provisioning below always runs under that
        // lock -- do NOT acquire it here, `tokio::sync::RwLock` is not reentrant.
        crate::crypto::provision_test_at_rest_key_under_write_guard();
        let d = std::env::temp_dir().join(format!("eg-crossmodal-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d.to_string_lossy().to_string()
    }

    /// A stage op answers `Bool`; `Commit` declares its unkeyed outcome as the JSON
    /// boolean, the same bytes on the wire.
    fn as_bool(r: Response) -> Option<bool> {
        match r.result {
            Some(ResultPayload::Bool(b))
            | Some(ResultPayload::Json(serde_json::Value::Bool(b))) => Some(b),
            _ => None,
        }
    }

    async fn txn_handle(
        state: &Arc<RwLock<ServerState>>,
        req_id: u64,
        _caller: Option<&str>,
        method: Method,
    ) -> Result<Response, Method> {
        let context = VerifiedRequestContext::verified_for_test(TEST_AGENT);
        txn_try_handle(state, req_id, TEST_AGENT, &context, method).await
    }

    /// Drive BeginTxn(g) → TxnAddNode(node) → TxnAddEmbedding → TxnBlobRef, returning
    /// the txn id (the staged cross-modal write-set is ready to Commit).
    async fn stage_crossmodal(
        state: &Arc<RwLock<ServerState>>,
        graph: &str,
        node: &str,
        digest: &str,
    ) -> String {
        let begin = txn_handle(
            state,
            1,
            None,
            Method::BeginTxn {
                graph: Some(graph.to_string()),
                isolation: None,
            },
        )
        .await
        .unwrap();
        let txn_id = match begin.result {
            Some(ResultPayload::String(id)) => id,
            other => panic!("BeginTxn id, got {other:?}"),
        };
        assert_eq!(
            as_bool(
                txn_handle(
                    state,
                    2,
                    None,
                    Method::TxnAddNode {
                        txn_id: txn_id.clone(),
                        node_id: node.to_string(),
                        properties_msgpack: props(serde_json::json!({"type": "Media"})),
                        graph: None,
                    },
                )
                .await
                .unwrap()
            ),
            Some(true)
        );
        assert_eq!(
            as_bool(
                txn_handle(
                    state,
                    3,
                    None,
                    Method::TxnAddEmbedding {
                        txn_id: txn_id.clone(),
                        node_id: node.to_string(),
                        embedding: vec![0.1, 0.2, 0.3],
                        graph: None,
                    },
                )
                .await
                .unwrap()
            ),
            Some(true)
        );
        assert_eq!(
            as_bool(
                txn_handle(
                    state,
                    4,
                    None,
                    Method::TxnBlobRef {
                        txn_id: txn_id.clone(),
                        node_id: node.to_string(),
                        digest: digest.to_string(),
                        graph: None,
                    },
                )
                .await
                .unwrap()
            ),
            Some(true)
        );
        txn_id
    }

    /// HAPPY: a cross-modal txn (node + vector + blob-ref) commits atomically — ALL
    /// modalities land durably in ONE WriteTransaction and survive a reload.
    #[tokio::test(flavor = "multi_thread")]
    async fn crossmodal_txn_commits_all_modalities_atomically() {
        // Held for the whole test: `cm_dir` provisions `EPISTEMIC_GRAPH_ENCRYPTION_KEY`
        // once (process-global) and this test's backend opens depend on it staying set
        // throughout — see `crate::crypto::acquire_test_env_lock`'s doc.
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = cm_dir("happy");
        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), 64).unwrap());
        let state = new_state(Some(dir.clone()));
        {
            let mut s = state.write().await;
            let _ = s.registry.create_graph("media", GraphType::Global, None);
            s.persistence = Some(backend.clone());
        }

        let txn_id = stage_crossmodal(&state, "media", "m1", "sha256:abc").await;
        // Nothing applied before commit.
        {
            let s = state.read().await;
            let core = s.registry.get("media").unwrap().core.clone();
            assert!(!core.has_node("m1"), "no apply before commit");
            assert_eq!(
                core.semantic_store.read().len(),
                0,
                "no vector before commit"
            );
        }

        // COMMIT — all three modalities land atomically.
        assert_eq!(
            as_bool(
                txn_handle(
                    &state,
                    5,
                    None,
                    Method::Commit {
                        txn_id,
                        idempotency_key: None
                    }
                )
                .await
                .unwrap()
            ),
            Some(true),
            "cross-modal commit"
        );

        // In-memory: node + vector + blob-ref property all present.
        {
            let s = state.read().await;
            let core = s.registry.get("media").unwrap().core.clone();
            assert!(core.has_node("m1"), "node landed");
            assert_eq!(core.semantic_store.read().len(), 1, "vector landed");
            let blob = core.get_node_properties("m1").unwrap();
            let p: serde_json::Map<String, serde_json::Value> =
                rmp_serde::from_slice(&blob).unwrap();
            assert_eq!(
                p.get("__blob__").and_then(|v| v.as_str()),
                Some("sha256:abc")
            );
        }
        backend.shutdown();
        drop(backend);
        // `state` holds a clone of the backend Arc; drop it so the LAST handle is gone
        // and the exclusive per-process redb file lock is released before the reopen.
        drop(state);

        // Reload from redb: every modality is DURABLE (the one WriteTransaction).
        let backend2: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), 64).unwrap());
        let state2 = new_state(Some(dir.clone()));
        backend2.load_all(&state2).await.unwrap();
        {
            let s = state2.read().await;
            let core = s.registry.get("media").unwrap().core.clone();
            assert!(core.has_node("m1"), "node durable");
            assert_eq!(core.semantic_store.read().len(), 1, "vector durable");
            let blob = core.get_node_properties("m1").unwrap();
            let p: serde_json::Map<String, serde_json::Value> =
                rmp_serde::from_slice(&blob).unwrap();
            assert_eq!(
                p.get("__blob__").and_then(|v| v.as_str()),
                Some("sha256:abc"),
                "blob-ref durable"
            );
        }
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A backend whose cross-modal DURABLE COMMIT always FAILS — used to prove the
    /// handler rolls back ALL modalities (applies nothing in-memory) on a
    /// durable-commit failure: no partial cross-modal commit
    /// (CONCEPT:EG-KG.txn.reader-never-sees-node).
    ///
    /// Everything that is NOT the injected failure delegates to a real
    /// `RedbBackend`, including the `as_redb` downcast. That delegation is load
    /// bearing twice over, and both halves were missing:
    ///
    /// * `as_redb` defaults to `None` on the trait (`persistence/mod.rs`), and
    ///   `handlers::txn::begin_txn_lifecycle_receipt` refuses every
    ///   txn-lifecycle method — `BeginTxn` included — without it ("transaction
    ///   lifecycle requires durable redb"). So this double failed the very FIRST
    ///   call of `stage_crossmodal`, and both tests using it died on
    ///   `panic!("BeginTxn id, got None")` before reaching the rollback they
    ///   name. The staging half of the scenario was never exercised at all.
    /// * the commit path calls `commit_mutation_batch_crossmodal`, not the
    ///   lower-level `commit_crossmodal` this double used to override. Overriding
    ///   only the latter left the injection attached to a method the handler no
    ///   longer calls: the commit would have failed anyway, but on the trait's
    ///   own "backend does not support atomic cross-modal MutationBatch commits"
    ///   default — i.e. the test would have asserted rollback after a
    ///   NOT-IMPLEMENTED error rather than after the mid-way durable failure it
    ///   describes. Both are overridden now, with the same injected message, so
    ///   whichever seam a future refactor routes through, the failure the test
    ///   names is the failure it gets.
    struct FailingBackend {
        inner: Arc<RedbBackend>,
    }

    #[async_trait::async_trait]
    impl PersistenceBackend for FailingBackend {
        async fn load_all(&self, s: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
            self.inner.load_all(s).await
        }
        async fn record_durable(&self, g: &str, m: &Method) -> Result<(), String> {
            self.inner.record_durable(g, m).await
        }
        fn as_redb(&self) -> Option<&RedbBackend> {
            self.inner.as_redb()
        }
        async fn commit_crossmodal(
            &self,
            _g: &str,
            _m: &[Method],
            _v: &[(String, Vec<f32>)],
            _b: &[(String, String)],
            _meas: &[crate::MeasurementBatch],
        ) -> Result<(), String> {
            // Simulate a mid-way durable failure: NOTHING is written to redb.
            Err("injected durable commit failure".to_string())
        }
        async fn commit_mutation_batch_crossmodal(
            &self,
            _args: crate::server::persistence::CrossModalCommitArgs<'_>,
        ) -> Result<crate::server::persistence::MutationBatchCommit, String> {
            // The seam the cross-modal Commit handler actually calls.
            Err("injected durable commit failure".to_string())
        }
        fn shutdown(&self) {
            self.inner.shutdown()
        }
    }

    /// ROLLBACK: a cross-modal txn whose durable commit FAILS mid-way applies NONE of
    /// its modalities — no node, no vector, no blob-ref (no partial commit).
    #[tokio::test(flavor = "multi_thread")]
    async fn crossmodal_txn_rolls_back_all_modalities_on_failure() {
        // See `crossmodal_txn_commits_all_modalities_atomically` above: held for the
        // whole test.
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = cm_dir("rollback");
        let inner = Arc::new(RedbBackend::open(dir.clone(), 64).unwrap());
        let backend: Arc<dyn PersistenceBackend> = Arc::new(FailingBackend {
            inner: inner.clone(),
        });
        let state = new_state(Some(dir.clone()));
        {
            let mut s = state.write().await;
            let _ = s.registry.create_graph("media", GraphType::Global, None);
            s.persistence = Some(backend.clone());
        }

        let txn_id = stage_crossmodal(&state, "media", "m1", "sha256:def").await;

        // COMMIT must FAIL (the durable barrier errored) → Response is an error.
        let resp = txn_handle(
            &state,
            5,
            None,
            Method::Commit {
                txn_id,
                idempotency_key: None,
            },
        )
        .await
        .unwrap();
        assert!(resp.error.is_some(), "commit surfaces the durable failure");
        assert!(resp.result.is_none(), "no Bool ack on a failed commit");

        // NO PARTIAL COMMIT: NONE of the modalities applied in-memory.
        {
            let s = state.read().await;
            let core = s.registry.get("media").unwrap().core.clone();
            assert!(!core.has_node("m1"), "node rolled back");
            assert_eq!(core.semantic_store.read().len(), 0, "vector rolled back");
        }

        // And NONE durable in redb either (the WriteTransaction never committed).
        inner.shutdown();
        drop(inner);
        drop(backend);
        // `state` holds a clone of the wrapper backend (which owns `inner`); drop it so
        // the last handle is gone and the redb file lock releases before the reopen.
        drop(state);
        let backend2: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), 64).unwrap());
        let state2 = new_state(Some(dir.clone()));
        backend2.load_all(&state2).await.unwrap();
        {
            let s = state2.read().await;
            let durable_node = s
                .registry
                .get("media")
                .map(|e| e.core.has_node("m1"))
                .unwrap_or(false);
            assert!(!durable_node, "node never landed durably");
        }
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Extended cross-modal ACID: 5 modalities in ONE wtx (CONCEPT:EG-KG.backend.cross-modal-atomic-commit/361/362) ──

    /// A data-independent CONSTRUCT that yields exactly one triple
    /// `<urn:a> <urn:p> <urn:b>` via an inline VALUES row (needs no committed data), so
    /// its staged lowering is a deterministic node+node+edge write.
    #[cfg(all(feature = "tsdb", feature = "sparql"))]
    const CONSTRUCT_Q: &str =
        "CONSTRUCT { ?x <urn:p> ?y } WHERE { VALUES (?x ?y) { (<urn:a> <urn:b>) } }";

    /// Encode a `Vec<(i64, Vec<f64>)>` measurement batch to the wire blob.
    #[cfg(all(feature = "tsdb", feature = "sparql"))]
    fn meas_points(pts: &[(i64, Vec<f64>)]) -> Vec<u8> {
        rmp_serde::to_vec(&pts.to_vec()).unwrap()
    }

    /// Stage all FIVE modalities into a fresh txn: graph node + embedding + blob-ref +
    /// time-series measurement + SPARQL CONSTRUCT triple. Returns the txn id.
    #[cfg(all(feature = "tsdb", feature = "sparql"))]
    async fn stage_five_modalities(
        state: &Arc<RwLock<ServerState>>,
        graph: &str,
        node: &str,
        digest: &str,
        series: &str,
        points: &[(i64, Vec<f64>)],
    ) -> String {
        let begin = txn_handle(
            state,
            1,
            None,
            Method::BeginTxn {
                graph: Some(graph.to_string()),
                isolation: None,
            },
        )
        .await
        .unwrap();
        let txn_id = match begin.result {
            Some(ResultPayload::String(id)) => id,
            other => panic!("BeginTxn id, got {other:?}"),
        };
        let ok = |r: Response| assert_eq!(as_bool(r), Some(true));
        ok(txn_handle(
            state,
            2,
            None,
            Method::TxnAddNode {
                txn_id: txn_id.clone(),
                node_id: node.to_string(),
                properties_msgpack: props(serde_json::json!({"type": "Media"})),
                graph: None,
            },
        )
        .await
        .unwrap());
        ok(txn_handle(
            state,
            3,
            None,
            Method::TxnAddEmbedding {
                txn_id: txn_id.clone(),
                node_id: node.to_string(),
                embedding: vec![0.1, 0.2, 0.3],
                graph: None,
            },
        )
        .await
        .unwrap());
        ok(txn_handle(
            state,
            4,
            None,
            Method::TxnBlobRef {
                txn_id: txn_id.clone(),
                node_id: node.to_string(),
                digest: digest.to_string(),
                graph: None,
            },
        )
        .await
        .unwrap());
        ok(txn_handle(
            state,
            5,
            None,
            Method::TxnAddMeasurement {
                txn_id: txn_id.clone(),
                series: series.to_string(),
                points: meas_points(points),
                graph: None,
            },
        )
        .await
        .unwrap());
        ok(txn_handle(
            state,
            6,
            None,
            Method::TxnConstruct {
                txn_id: txn_id.clone(),
                sparql: CONSTRUCT_Q.to_string(),
                graph: None,
            },
        )
        .await
        .unwrap());
        txn_id
    }

    /// CAPSTONE: one txn stages node + embedding + blob-ref + measurement + CONSTRUCT
    /// triple; `Commit` lands ALL FIVE atomically in ONE redb `WriteTransaction`; every
    /// modality is durably present after a full backend reload (CONCEPT:EG-KG.backend.cross-modal-atomic-commit/361/362).
    #[cfg(all(feature = "tsdb", feature = "sparql"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn five_modality_atomic_commit() {
        use eg_tsdb::store::SeriesStore;

        // See `crossmodal_txn_commits_all_modalities_atomically` above: held for the
        // whole test.
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = cm_dir("five");
        let points = vec![
            (1_000_000_000i64, vec![10.0]),
            (2_000_000_000i64, vec![20.0]),
        ];
        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), 64).unwrap());
        // The measurement modality of this cross-modal commit is written to the tsdb
        // SeriesStore, so the state MUST carry one (same setup the measurement +
        // reconciliation tests use) — without it the Commit fails writing the tsdb leg.
        let series_store = Arc::new(
            SeriesStore::open_in_dir(
                std::path::Path::new(&dir),
                crate::store_authority::process_verifier(),
                crate::store_authority::process_authority().principal(),
                &crate::store_authority::process_authority().proof(),
            )
            .unwrap(),
        );
        let state = new_state(Some(dir.clone()));
        {
            let mut s = state.write().await;
            let _ = s.registry.create_graph("media", GraphType::Global, None);
            s.persistence = Some(backend.clone());
            s.tsdb_store = Some(series_store.clone());
        }

        let txn_id =
            stage_five_modalities(&state, "media", "m1", "sha256:abc", "sensor", &points).await;
        // Nothing applied before commit.
        {
            let s = state.read().await;
            let core = s.registry.get("media").unwrap().core.clone();
            assert!(!core.has_node("m1"), "no apply before commit");
            assert!(!core.has_node("<urn:a>"), "no CONSTRUCT node before commit");
            assert_eq!(
                core.semantic_store.read().len(),
                0,
                "no vector before commit"
            );
        }

        assert_eq!(
            as_bool(
                txn_handle(
                    &state,
                    7,
                    None,
                    Method::Commit {
                        txn_id,
                        idempotency_key: None
                    }
                )
                .await
                .unwrap()
            ),
            Some(true),
            "five-modality commit"
        );

        // In-memory: graph modalities all present (node + vector + blob + CONSTRUCT edge).
        {
            let s = state.read().await;
            let core = s.registry.get("media").unwrap().core.clone();
            assert!(core.has_node("m1"), "node landed");
            assert_eq!(core.semantic_store.read().len(), 1, "vector landed");
            assert!(
                core.has_node("<urn:a>") && core.has_node("<urn:b>"),
                "CONSTRUCT nodes landed"
            );
            let blob = core.get_node_properties("m1").unwrap();
            let p: serde_json::Map<String, serde_json::Value> =
                rmp_serde::from_slice(&blob).unwrap();
            assert_eq!(
                p.get("__blob__").and_then(|v| v.as_str()),
                Some("sha256:abc")
            );
        }
        backend.shutdown();
        drop(backend);
        // `state` holds a clone of the backend Arc; drop it so the last handle is gone
        // and the redb file lock releases before the same-dir reopens below.
        drop(state);

        // Measurements are durable in the authoritative shard's SERIES tables
        // (same wtx, not series.redb).
        {
            // graph-0.redb is a GraphShard owner file; reopen it through its
            // sole owner and read the file-wide SERIES tables from the control
            // scope. Opening it as a standalone SeriesStore would ask the
            // storage kernel for a different owner manifest.
            let shard =
                Shard::open(std::path::Path::new(&dir).join(shard_filename(0)).as_path()).unwrap();
            let read = shard.control_read().unwrap();
            let key = direct_test_series_key("media", "sensor");
            let meta = eg_tsdb::store::meta_in_rtx(&read, &key)
                .unwrap()
                .expect("series durable");
            assert_eq!(meta.count, 2, "both measurement points durable");
            let scanned = eg_tsdb::store::range_in_rtx(
                &read,
                &key,
                eg_tsdb::point::Ts::MIN,
                eg_tsdb::point::Ts::MAX,
            )
            .unwrap();
            assert_eq!(scanned.len(), 2, "measurement points readable post-reload");
            assert_eq!(scanned[0].values, vec![10.0]);
            assert_eq!(scanned[1].values, vec![20.0]);
        }

        // Reload the graph tier: node + vector + blob-ref + CONSTRUCT triple all durable.
        let backend2: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), 64).unwrap());
        let state2 = new_state(Some(dir.clone()));
        backend2.load_all(&state2).await.unwrap();
        {
            let s = state2.read().await;
            let core = s.registry.get("media").unwrap().core.clone();
            assert!(core.has_node("m1"), "node durable");
            assert_eq!(core.semantic_store.read().len(), 1, "vector durable");
            assert!(
                core.has_node("<urn:a>") && core.has_node("<urn:b>"),
                "CONSTRUCT nodes durable"
            );
            let blob = core.get_node_properties("m1").unwrap();
            let p: serde_json::Map<String, serde_json::Value> =
                rmp_serde::from_slice(&blob).unwrap();
            assert_eq!(
                p.get("__blob__").and_then(|v| v.as_str()),
                Some("sha256:abc"),
                "blob durable"
            );
        }
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// ATOMICITY: a five-modality txn whose durable commit FAILS mid-way (the always-fails
    /// backend, injected AFTER the measurement is staged) lands NONE of the five — no node,
    /// no vector, no CONSTRUCT triple in-memory, and NO series durable (CONCEPT:EG-KG.backend.cross-modal-atomic-commit).
    #[cfg(all(feature = "tsdb", feature = "sparql"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn five_modality_rolls_back_all_on_failure() {
        // See `crossmodal_txn_commits_all_modalities_atomically` above: held for the
        // whole test.
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = cm_dir("five-rollback");
        let points = vec![(1_000_000_000i64, vec![10.0])];
        let inner = Arc::new(RedbBackend::open(dir.clone(), 64).unwrap());
        let backend: Arc<dyn PersistenceBackend> = Arc::new(FailingBackend {
            inner: inner.clone(),
        });
        let state = new_state(Some(dir.clone()));
        {
            let mut s = state.write().await;
            let _ = s.registry.create_graph("media", GraphType::Global, None);
            s.persistence = Some(backend.clone());
        }

        let txn_id =
            stage_five_modalities(&state, "media", "m1", "sha256:def", "sensor", &points).await;

        // COMMIT must FAIL (the durable barrier errored) → error Response, no ack.
        let resp = txn_handle(
            &state,
            7,
            None,
            Method::Commit {
                txn_id,
                idempotency_key: None,
            },
        )
        .await
        .unwrap();
        assert!(resp.error.is_some(), "commit surfaces the durable failure");
        assert!(resp.result.is_none(), "no Bool ack on a failed commit");

        // NO PARTIAL COMMIT: none of the graph modalities applied in-memory.
        {
            let s = state.read().await;
            let core = s.registry.get("media").unwrap().core.clone();
            assert!(!core.has_node("m1"), "node rolled back");
            assert!(!core.has_node("<urn:a>"), "CONSTRUCT triple rolled back");
            assert_eq!(core.semantic_store.read().len(), 0, "vector rolled back");
        }

        inner.shutdown();
        drop(inner);
        drop(backend);

        // And the measurement never landed durably either (the wtx never committed).
        {
            let shard =
                Shard::open(std::path::Path::new(&dir).join(shard_filename(0)).as_path()).unwrap();
            let read = shard.control_read().unwrap();
            let key = direct_test_series_key("media", "sensor");
            assert!(
                eg_tsdb::store::meta_in_rtx(&read, &key).unwrap().is_none(),
                "series never landed durably"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// EG-P0-4 (CONCEPT:EG-KG.backend.ts-served-materialize) — the canonical time-series
    /// read-path unification proof. A measurement committed through the cross-modal txn
    /// path (staged alongside a plain node write in ONE txn — a measurement alone already
    /// makes a txn cross-modal per `GraphTxnState::is_cross_modal`) is:
    ///  1. durable in the authoritative shard's SERIES tables (the atomic barrier,
    ///     unchanged — verified directly against that file, exactly like
    ///     `five_modality_atomic_commit`);
    ///  2. ALSO visible through the PUBLIC `Method::TsRange` read path immediately after
    ///     `Commit` acks — the actual gap this workstream closes (before, a cross-modal
    ///     measurement was durable yet permanently unreachable from `TsRange`/`TsScan`);
    ///  3. STILL visible via `TsRange` after a full process restart (drop + reopen BOTH
    ///     the redb backend AND the served time-series store from the same persist dir,
    ///     on a brand-new `ServerState`) — proving the served-store materialization is
    ///     itself a committed durable write, not an in-memory-only mirror that a restart
    ///     would lose.
    #[cfg(feature = "tsdb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn crossmodal_measurement_visible_via_public_tsrange_post_commit_and_restart() {
        use crate::protocol::ResultPayload;
        use eg_tsdb::store::SeriesStore;

        const SECRET: &str = "ts-unify-secret";
        // Held for the whole test — this one literally reopens the backend
        // ("...post_commit_and_restart"), so cipher stability across BOTH opens is
        // exactly what this lock guarantees. See
        // `crossmodal_txn_commits_all_modalities_atomically` above.
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = cm_dir("ts-unify");
        let points = vec![
            (1_000_000_000i64, vec![10.0]),
            (2_000_000_000i64, vec![20.0]),
            (3_000_000_000i64, vec![30.0]),
        ];

        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), 64).unwrap());
        let series_store = Arc::new(
            SeriesStore::open_in_dir(
                std::path::Path::new(&dir),
                crate::store_authority::process_verifier(),
                crate::store_authority::process_authority().principal(),
                &crate::store_authority::process_authority().proof(),
            )
            .unwrap(),
        );
        let state = new_state(Some(dir.clone()));
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            let _ = s.registry.create_graph("media", GraphType::Global, None);
            s.persistence = Some(backend.clone());
            s.tsdb_store = Some(series_store.clone());
        }

        let req = |id: u64, method: Method| current_request(SECRET, id, "media", method);

        // Stage a node + a measurement in ONE cross-modal txn, then commit.
        let begin = dispatch_on_heap(
            &state,
            req(
                1,
                Method::BeginTxn {
                    graph: Some("media".to_string()),
                    isolation: None,
                },
            ),
        )
        .await;
        let txn_id = match begin.result {
            Some(ResultPayload::String(id)) => id,
            other => panic!("BeginTxn id, got {other:?}"),
        };
        let r = dispatch_on_heap(
            &state,
            req(
                2,
                Method::TxnAddNode {
                    txn_id: txn_id.clone(),
                    node_id: "m1".into(),
                    properties_msgpack: props(serde_json::json!({"type": "Sensor"})),
                    graph: None,
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "stage node: {:?}", r.error);
        let r = dispatch_on_heap(
            &state,
            req(
                3,
                Method::TxnAddMeasurement {
                    txn_id: txn_id.clone(),
                    series: "sensor.ts-unify".to_string(),
                    // Encoded inline (NOT the `sparql`-gated `meas_points` helper) so this
                    // test only needs `tsdb`, matching the `#[cfg]` above.
                    points: rmp_serde::to_vec(&points).unwrap(),
                    graph: None,
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "stage measurement: {:?}", r.error);

        let commit = dispatch_on_heap(
            &state,
            req(
                4,
                Method::Commit {
                    txn_id,
                    idempotency_key: None,
                },
            ),
        )
        .await;
        assert_eq!(
            as_bool(commit),
            Some(true),
            "cross-modal commit must succeed"
        );

        // (1) POST-COMMIT visibility through the PUBLIC TsRange read path (the served
        // series.redb) — the actual gap this workstream closes. Checked FIRST, while
        // `backend`/`series_store` are both still live (they're two independent redb
        // files/handles, so no lock conflict).
        let ts_range = || {
            req(
                5,
                Method::TsRange {
                    series_id: "sensor.ts-unify".to_string(),
                    from: 0,
                    to: i64::MAX,
                },
            )
        };
        let decode_ts = |r: Response| -> Vec<(i64, Vec<f64>)> {
            assert!(r.error.is_none(), "TsRange error: {:?}", r.error);
            match r.result {
                Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
                other => panic!("expected Raw TsRange result, got {other:?}"),
            }
        };
        let got = decode_ts(dispatch_on_heap(&state, ts_range()).await);
        assert_eq!(
            got, points,
            "measurement committed via the cross-modal txn path must be visible through \
             the PUBLIC TsRange API immediately after Commit"
        );

        // (2) Durable in the authoritative shard too — the atomic barrier.
        // redb holds an EXCLUSIVE per-process file lock, so `backend` (which owns the
        // live shard handle) must release it first — exactly the ordering
        // `five_modality_atomic_commit` uses to open a second, direct handle on the
        // same file.
        backend.shutdown();
        drop(backend);
        {
            let shard =
                Shard::open(std::path::Path::new(&dir).join(shard_filename(0)).as_path()).unwrap();
            let read = shard.control_read().unwrap();
            let key = envelope_test_series_key("media", "sensor.ts-unify");
            let meta = eg_tsdb::store::meta_in_rtx(&read, &key)
                .unwrap()
                .expect("measurement durable in the authoritative shard");
            assert_eq!(meta.count, 3, "all 3 points durable in the shard");
        }

        // (3) RESTART: drop + reopen BOTH stores from the SAME persist dir on a FRESH
        // ServerState, then re-run the SAME public TsRange call.
        drop(series_store);
        drop(state);

        let backend2: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), 64).unwrap());
        let state2 = new_state(Some(dir.clone()));
        backend2.load_all(&state2).await.unwrap();
        let series_store2 = Arc::new(
            SeriesStore::open_in_dir(
                std::path::Path::new(&dir),
                crate::store_authority::process_verifier(),
                crate::store_authority::process_authority().principal(),
                &crate::store_authority::process_authority().proof(),
            )
            .unwrap(),
        );
        {
            let mut s = state2.write().await;
            s.auth_secret = SECRET.to_string();
            s.persistence = Some(backend2.clone());
            s.tsdb_store = Some(series_store2.clone());
        }

        let got2 = decode_ts(dispatch_on_heap(&state2, ts_range()).await);
        assert_eq!(
            got2, points,
            "measurement must STILL be visible through the PUBLIC TsRange API after a \
             full restart (served store reopened from disk, not rebuilt from RAM)"
        );

        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// L16 startup reconciliation (CONCEPT:EG-KG.backend.ts-startup-reconcile) — the proof that closes the
    /// EG-P0-4 residual. Simulates a crash STRICTLY BETWEEN the two commits by landing a
    /// measurement batch in the authoritative shard via the low-level `commit_crossmodal` (the atomic
    /// barrier alone) WITHOUT going through `handlers::txn::commit_cross_modal_txn` (which
    /// is what performs the served-store replay) — so the served `series.redb` never sees
    /// it, exactly the documented gap. Asserts:
    ///  1. Before reconciliation, the measurement is invisible via the PUBLIC `TsRange`.
    ///  2. `RedbBackend::reconcile_time_series` finds + replays it; `TsRange` now returns
    ///     it.
    ///  3. Running reconciliation a SECOND time is a true no-op (nothing reconciled) and
    ///     does NOT duplicate the points — proving idempotency.
    #[cfg(feature = "tsdb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn startup_reconciliation_closes_the_crash_window_gap() {
        use crate::protocol::ResultPayload;
        use eg_tsdb::store::SeriesStore;

        const SECRET: &str = "ts-reconcile-secret";
        // See `crossmodal_txn_commits_all_modalities_atomically` above: held for the
        // whole test.
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = cm_dir("ts-reconcile");
        let points: Vec<(i64, Vec<f64>)> = vec![
            (1_000_000_000i64, vec![1.0]),
            (2_000_000_000i64, vec![2.0]),
            (3_000_000_000i64, vec![3.0]),
        ];
        const SERIES: &str = "sensor.ts-reconcile";
        const BUCKET_NS: u64 = 3_600_000_000_000; // 1h — matches DEFAULT_MEASUREMENT_BUCKET_NS.

        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), 64).unwrap());
        let series_store = Arc::new(
            SeriesStore::open_in_dir(
                std::path::Path::new(&dir),
                crate::store_authority::process_verifier(),
                crate::store_authority::process_authority().principal(),
                &crate::store_authority::process_authority().proof(),
            )
            .unwrap(),
        );
        let state = new_state(Some(dir.clone()));
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            let _ = s.registry.create_graph("media", GraphType::Global, None);
            s.persistence = Some(backend.clone());
            s.tsdb_store = Some(series_store.clone());
        }

        // ── Simulate the crash window: land the measurement ONLY in the authoritative
        // shard, via the
        // atomic barrier alone, bypassing the txn-handler replay step entirely. ──
        let measurement: crate::MeasurementBatch = (
            envelope_test_series_key("media", SERIES),
            1,
            BUCKET_NS,
            vec!["value".to_string()],
            points.clone(),
        );
        backend
            .commit_crossmodal("media", &[], &[], &[], std::slice::from_ref(&measurement))
            .await
            .expect("authoritative-shard-only commit must succeed");

        let req = |id: u64, method: Method| current_request(SECRET, id, "media", method);
        let ts_range = |id: u64| {
            req(
                id,
                Method::TsRange {
                    series_id: SERIES.to_string(),
                    from: 0,
                    to: i64::MAX,
                },
            )
        };
        let decode_ts = |r: Response| -> Vec<(i64, Vec<f64>)> {
            assert!(r.error.is_none(), "TsRange error: {:?}", r.error);
            match r.result {
                Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
                other => panic!("expected Raw TsRange result, got {other:?}"),
            }
        };

        // (1) BEFORE reconciliation: invisible through the PUBLIC TsRange — the gap
        // `commit_crossmodal` alone (without the txn-handler replay step) leaves open.
        // (Durability in the authoritative shard itself is proven by reconciliation
        // finding + replaying exactly 3 points — a SECOND `Database` handle can't be
        // opened directly on the shard here to double-check, since `backend` still
        // holds redb's exclusive per-process file lock on it, same constraint the
        // EG-P0-4 test works around by dropping its backend first.)
        let before = decode_ts(dispatch_on_heap(&state, ts_range(1)).await);
        assert!(
            before.is_empty(),
            "before reconciliation, a measurement landed only via the authoritative shard \
             commit (the simulated crash) must NOT yet be visible through TsRange"
        );

        // (2) Reconcile: the redb-backed reader is reached the SAME way production code
        // does — downcast the trait object via `as_redb()`.
        let redb = backend.as_redb().expect("redb backend");
        let report = redb
            .reconcile_time_series(&series_store)
            .await
            .expect("reconciliation must succeed");
        assert_eq!(
            report.series_reconciled, 1,
            "exactly one series needed replay"
        );
        assert_eq!(report.points_replayed, 3, "all 3 points replayed");

        let after = decode_ts(dispatch_on_heap(&state, ts_range(2)).await);
        assert_eq!(
            after, points,
            "after reconciliation, the measurement must be visible through the PUBLIC \
             TsRange API — the crash-window gap is closed"
        );

        // (3) IDEMPOTENCY: reconciling again is a true no-op — nothing to replay, and
        // TsRange returns the SAME points (no duplicates).
        let report2 = redb
            .reconcile_time_series(&series_store)
            .await
            .expect("second reconciliation must succeed");
        assert_eq!(
            report2.series_reconciled, 0,
            "a converged series must not be re-reconciled"
        );
        assert_eq!(report2.points_replayed, 0);
        let after2 = decode_ts(dispatch_on_heap(&state, ts_range(3)).await);
        assert_eq!(
            after2, points,
            "running reconciliation twice must not duplicate any point"
        );

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// AuditVerify dispatch round-trip (CONCEPT:EG-KG.sharding.row-level-security): durable writes build a
    /// hash-chained audit log; `Method::AuditVerify` over the served dispatch returns
    /// `ok=true`; tampering an entry makes the served verify report the break.
    #[cfg(feature = "security")]
    #[tokio::test]
    async fn audit_verify_dispatch_detects_tamper() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::protocol::{AuditReport, ResultPayload};

        const SECRET: &str = "audit-secret";
        let dir = std::env::temp_dir().join(format!("eg-audit-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let backend = Arc::new(RedbBackend::open(dir_s.clone(), 64).expect("open redb backend"));
        let state = new_state(Some(dir_s.clone()));
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            s.persistence = Some(backend.clone());
        }
        let req = |id: u64, method: Method| current_request(SECRET, id, "__commons__", method);

        // Two durable writes → two chained audit entries (commit-before-ack durable).
        for (rid, nid) in [(1u64, "n1"), (2, "n2")] {
            let r = dispatch_on_heap(
                &state,
                req(
                    rid,
                    Method::AddNode {
                        node_id: nid.into(),
                        properties_msgpack: props(serde_json::json!({"v": rid})),
                    },
                ),
            )
            .await;
            assert!(r.error.is_none(), "add failed: {:?}", r.error);
        }

        // Served AuditVerify ⇒ ok.
        let decode = |r: crate::protocol::Response| -> AuditReport {
            match r.result {
                Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
                other => panic!("expected raw AuditReport, got {other:?}"),
            }
        };
        let report = decode(dispatch_on_heap(&state, req(3, Method::AuditVerify)).await);
        assert!(report.ok, "clean chain should verify: {report:?}");
        assert_eq!(report.entries, 2);

        // Tamper the audit table directly under the writer-thread DB, then re-verify.
        backend
            .test_tamper_audit_entry(&crate::persist::sanitize("__commons__"), 0)
            .expect("tamper");
        let broken = decode(dispatch_on_heap(&state, req(4, Method::AuditVerify)).await);
        assert!(!broken.ok, "tamper should be detected");
        assert_eq!(broken.first_broken_seq, Some(0));

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// GetLedger RPC-boundary regression (BUG A1, 2026-08-12): `GetLedger` used to
    /// read the mutation ledger off the RLS-projected `core` shadowed inside
    /// `handlers::graph_ops::try_handle` (`GraphReadAuthority::project_core`),
    /// whose detached copy is built via `add_node_no_ledger`/`add_edge_no_ledger`
    /// and therefore NEVER carries a ledger at all -- see `build_projection`'s own
    /// doc in `server::access`. Because `security` (hence
    /// `GraphReadAuthority::is_active()`) is compiled into the DEFAULT `full`
    /// build, `GetLedger` returned `[]` on EVERY served request, regardless of
    /// how many mutations had actually committed -- indistinguishable from
    /// "nothing to sync" at every real production caller
    /// (`agent_utilities.workflows.epistemic_sync.flush_ledger_to_backend`).
    ///
    /// Commit N real mutations over the SAME served dispatch path this bug
    /// lived on, call `GetLedger`, and assert exactly N entries come back
    /// `populated: true` -- proving the fix reads the REAL, authoritative
    /// ledger (`raw_core`), not the RLS projection's permanently-empty one.
    /// `watermark` must be `0` here: well under the ledger's 100k cap, this
    /// instance has never dropped anything (see
    /// `eg_core::graph::tests::ledger_cap_drop_advances_the_watermark` for
    /// the cap-drop case itself -- that mechanism is a SEPARATE, real gap
    /// this fix does not paper over: the ledger remains a purely in-memory,
    /// ephemeral buffer, not a durable change log).
    #[tokio::test]
    async fn get_ledger_dispatch_returns_real_committed_mutations() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::protocol::{LedgerReadResult, ResultPayload};

        const SECRET: &str = "get-ledger-secret";
        let dir = std::env::temp_dir().join(format!("eg-get-ledger-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = Arc::new(RedbBackend::open(dir_s.clone(), 64).expect("open redb backend"));
        let state = new_state(Some(dir_s.clone()));
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            s.persistence = Some(backend.clone());
        }
        let req = |id: u64, method: Method| current_request(SECRET, id, "__commons__", method);

        // Three real, committed mutations over the served dispatch path.
        for (rid, nid) in [(1u64, "gl1"), (2, "gl2"), (3, "gl3")] {
            let r = dispatch_on_heap(
                &state,
                req(
                    rid,
                    Method::AddNode {
                        node_id: nid.into(),
                        properties_msgpack: props(serde_json::json!({"v": rid})),
                    },
                ),
            )
            .await;
            assert!(r.error.is_none(), "add failed: {:?}", r.error);
        }

        let decode = |r: crate::protocol::Response| -> LedgerReadResult {
            match r.result {
                Some(ResultPayload::Json(v)) => serde_json::from_value(v).unwrap(),
                other => panic!("expected a typed LedgerReadResult, got {other:?}"),
            }
        };
        let ledger = decode(dispatch_on_heap(&state, req(4, Method::GetLedger)).await);
        assert!(
            ledger.populated,
            "a real, committed ledger must be reported populated: {ledger:?}"
        );
        assert_eq!(
            ledger.entries.len(),
            3,
            "GetLedger must return exactly the 3 committed mutations, not the \
             RLS-projected permanently-empty ledger: {:?}",
            ledger.entries
        );
        assert_eq!(
            ledger.watermark, 0,
            "nothing has been dropped from this fresh, far-under-cap ledger"
        );
        for needle in ["gl1", "gl2", "gl3"] {
            assert!(
                ledger.entries.iter().any(|e| e.contains(needle)),
                "missing {needle} in ledger entries: {:?}",
                ledger.entries
            );
        }

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The genuinely-empty case is explicitly typed too (BUG A1, 2026-08-12): a
    /// freshly-created graph with ZERO committed mutations answers `populated:
    /// true, entries: []` -- a REAL empty ledger, which a bare `Vec<String>`
    /// could never distinguish from "the read failed" and a typed
    /// [`LedgerReadResult`] now can. No prior write, no persistence backend
    /// needed -- `GetLedger` is the very first request this graph ever sees.
    #[tokio::test]
    async fn get_ledger_dispatch_types_a_genuinely_empty_ledger_distinctly() {
        use crate::protocol::{LedgerReadResult, ResultPayload};

        const SECRET: &str = "get-ledger-empty-secret";
        let state = new_state(None);
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
        }
        let req = |id: u64, method: Method| current_request(SECRET, id, "__commons__", method);

        let decode = |r: crate::protocol::Response| -> LedgerReadResult {
            match r.result {
                Some(ResultPayload::Json(v)) => serde_json::from_value(v).unwrap(),
                other => panic!("expected a typed LedgerReadResult, got {other:?}"),
            }
        };
        let ledger = decode(dispatch_on_heap(&state, req(1, Method::GetLedger)).await);
        assert!(
            ledger.populated,
            "a genuinely empty ledger is still POPULATED (the read succeeded, \
             there is simply nothing in it): {ledger:?}"
        );
        assert!(
            ledger.entries.is_empty(),
            "expected zero entries on a never-mutated graph: {:?}",
            ledger.entries
        );
        assert_eq!(
            ledger.watermark, 0,
            "a never-mutated graph has never dropped anything"
        );
    }

    /// Provenance-anchor inclusion proof (CONCEPT:EG-KG.sharding.row-level-security, provenance anchoring) —
    /// the tamper-detection acceptance test: a `:ToolCall` node's window is
    /// anchored, its inclusion proof verifies against that anchor's chain-protected
    /// root, and
    /// an overwrite of that SAME node's durable content AFTER anchoring — through
    /// the ORDINARY served write path, not a raw byte-flip — makes the SAME
    /// anchor's inclusion proof fail. A node that was never in the window
    /// reports `included=false` rather than a false pass, and the audit chain
    /// itself (both anchor entries) still verifies clean throughout.
    #[cfg(feature = "security")]
    #[tokio::test]
    async fn provenance_anchor_inclusion_proof_detects_tamper() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::protocol::{AuditReport, MerkleInclusionReport, ResultPayload};
        use crate::server::persistence::provenance_anchor;

        const SECRET: &str = "provenance-secret";
        let dir = std::env::temp_dir().join(format!("eg-provenance-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let backend = Arc::new(RedbBackend::open(dir_s.clone(), 64).expect("open redb backend"));
        let state = new_state(Some(dir_s.clone()));
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            s.persistence = Some(backend.clone());
        }
        let req = |id: u64, method: Method| current_request(SECRET, id, "__commons__", method);

        // Seed a ToolCall, a RunTrace, and an ordinary (non-provenance) node.
        for (rid, nid, node_type) in [
            (1u64, "tc-1", "ToolCall"),
            (2, "rt-1", "RunTrace"),
            (3, "widget-1", "Widget"),
        ] {
            let r = dispatch_on_heap(
                &state,
                req(
                    rid,
                    Method::AddNode {
                        node_id: nid.into(),
                        properties_msgpack: props(
                            serde_json::json!({"node_type": node_type, "v": 1}),
                        ),
                    },
                ),
            )
            .await;
            assert!(r.error.is_none(), "seed add failed: {:?}", r.error);
        }

        // Sweep: anchors the __commons__ ToolCall+RunTrace window (Widget is out
        // of scope, so it never affects the anchored root).
        let anchored = provenance_anchor::sweep(&state).await;
        assert_eq!(
            anchored, 1,
            "exactly one graph (__commons__) should be freshly anchored"
        );

        let core = state
            .read()
            .await
            .registry
            .get("__commons__")
            .unwrap()
            .core
            .clone();
        let anchored_version =
            PersistenceBackend::read_mutation_graph_version(backend.as_ref(), "__commons__")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            anchored_version, 4,
            "three node writes and one anchor commit"
        );
        assert_eq!(
            core.version(),
            anchored_version,
            "anchor publishes the durable OCC fence"
        );
        assert_eq!(
            provenance_anchor::sweep(&state).await,
            0,
            "unchanged root is idle"
        );
        assert_eq!(
            core.version(),
            anchored_version,
            "idle sweep must not bump RAM"
        );
        assert_eq!(
            PersistenceBackend::read_mutation_graph_version(backend.as_ref(), "__commons__")
                .await
                .unwrap(),
            Some(anchored_version),
            "idle sweep must not bump authority"
        );

        let decode_report = |r: crate::protocol::Response| -> MerkleInclusionReport {
            match r.result {
                Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
                other => panic!("expected raw MerkleInclusionReport, got {other:?}"),
            }
        };

        // A clean, freshly-anchored ToolCall node verifies.
        let clean = decode_report(
            dispatch_on_heap(
                &state,
                req(
                    4,
                    Method::AuditProveInclusion {
                        node_id: "tc-1".to_string(),
                        anchor_seq: None,
                    },
                ),
            )
            .await,
        );
        assert!(clean.included, "tc-1 must be part of the anchor's window");
        assert!(clean.verified, "clean node should verify: {clean:?}");
        assert_eq!(clean.window_size, 2, "only tc-1 + rt-1 are in scope");
        let anchor_seq = clean.anchor_seq;

        // Overwrite tc-1's durable content through the ORDINARY served write path
        // (not a raw byte-flip) -- the realistic tamper/insider-edit scenario.
        let r = dispatch_on_heap(
            &state,
            req(
                5,
                Method::AddNode {
                    node_id: "tc-1".to_string(),
                    properties_msgpack: props(
                        serde_json::json!({"node_type": "ToolCall", "v": "TAMPERED"}),
                    ),
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "overwrite failed: {:?}", r.error);

        // Same anchor (explicit `anchor_seq`), same node id: now fails
        // verification -- the acceptance property.
        let tampered = decode_report(
            dispatch_on_heap(
                &state,
                req(
                    6,
                    Method::AuditProveInclusion {
                        node_id: "tc-1".to_string(),
                        anchor_seq: Some(anchor_seq),
                    },
                ),
            )
            .await,
        );
        assert!(
            tampered.included,
            "tc-1 is still part of that anchor's window"
        );
        assert!(
            !tampered.verified,
            "tampered node must fail inclusion verification: {tampered:?}"
        );
        assert_ne!(
            tampered.computed_root_sha256, tampered.anchored_root_sha256,
            "a tampered leaf must recompute a different root than the anchor"
        );
        assert_eq!(
            tampered.anchored_root_sha256, clean.anchored_root_sha256,
            "the ANCHORED root itself (chain-protected) must not change"
        );

        // A node that was never in the window reports included=false, never a
        // false "verified".
        let out_of_window = decode_report(
            dispatch_on_heap(
                &state,
                req(
                    7,
                    Method::AuditProveInclusion {
                        node_id: "widget-1".to_string(),
                        anchor_seq: Some(anchor_seq),
                    },
                ),
            )
            .await,
        );
        assert!(!out_of_window.included);
        assert!(!out_of_window.verified);

        // tc-1's overwrite changed the window's root, so a second sweep anchors
        // AGAIN (a second, distinct chain entry) -- and the audit chain itself
        // (both anchor entries, plus the ordinary mutation entries) still
        // verifies clean: provenance anchoring never breaks `AuditVerify`.
        let anchored_again = provenance_anchor::sweep(&state).await;
        assert_eq!(
            anchored_again, 1,
            "the tampered content changed the root, so it anchors again"
        );

        assert_eq!(
            core.version(),
            6,
            "overwrite and second anchor each publish once"
        );
        assert_eq!(
            PersistenceBackend::read_mutation_graph_version(backend.as_ref(), "__commons__")
                .await
                .unwrap(),
            Some(core.version()),
            "second anchor retains authoritative/serving parity"
        );

        let audit_report: AuditReport = match dispatch_on_heap(&state, req(8, Method::AuditVerify))
            .await
            .result
        {
            Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
            other => panic!("expected raw AuditReport, got {other:?}"),
        };
        assert!(
            audit_report.ok,
            "the audit chain itself (incl. both anchor entries) must still verify: {audit_report:?}"
        );

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── CONCEPT:EG-KG.backend.sharded-k-way-durable — sharded K-way durable writer ──────────────────────────

    /// Routing is a stable, deterministic FNV-1a — a graph maps to the SAME shard
    /// every process/restart (else its durable rows become unreachable), stays in
    /// `0..K`, and collapses to shard 0 under K<=1.
    #[test]
    fn shard_index_is_stable_and_bounded() {
        for k in [1usize, 2, 3, 4, 8] {
            for name in [
                "__commons__",
                "agent:planner",
                "g1",
                "enterprise-acme:billing",
            ] {
                let a = shard_index(name, k);
                let b = shard_index(name, k);
                assert_eq!(a, b, "routing must be deterministic for {name} (K={k})");
                assert!(a < k, "shard {a} out of range for K={k}");
            }
        }
        // K=1 always routes to the single shard 0.
        assert_eq!(shard_index("anything", 1), 0);
        assert_eq!(shard_index("anything", 0), 0);
        assert_eq!(shard_filename(0), "graph-0.redb");
        assert_eq!(shard_filename(2), "graph-2.redb");
    }

    /// K=1 uses the same indexed filename contract as every other shard count.
    #[tokio::test]
    async fn k1_uses_canonical_indexed_layout() {
        // Defensive: this test opens a backend twice (K=1 shard layout, then a plain
        // `open()` over the same dir after removal+recreation). Neither open ever
        // writes/reads an encrypted value, so an ambient key toggle between the two
        // opens is very unlikely to be observable here -- but hold the lock anyway so
        // this is never the crate's third instance of the "two opens, ambient key
        // mid-flight" class. See `crate::crypto::acquire_test_env_lock`'s doc.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-shard-k1-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open_with_shards(dir_s.clone(), 64, 1).expect("open K=1");
        assert_eq!(backend.shard_count(), 1);
        assert!(
            dir.join("graph-0.redb").exists(),
            "K=1 must use canonical graph-0.redb"
        );
        assert!(
            !dir.join("graph.redb").exists(),
            "normal startup must not create the retired graph.redb layout"
        );
        // cfg(test) auto-resolves to K=1, so a plain open() is the single-file path
        // too. Serialize vs the SHARDS-env-override test (and defensively clear the
        // var) so a concurrent test can't leak K into this layout assertion.
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
        let auto = {
            let _env = LINGER_ENV_LOCK.lock().unwrap();
            std::env::remove_var("EPISTEMIC_GRAPH_REDB_SHARDS");
            RedbBackend::open(dir_s.clone(), 64).expect("open auto")
        };
        assert_eq!(auto.shard_count(), 1, "cfg(test) default K=1");
        assert!(dir.join("graph-0.redb").exists());
        auto.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn normal_startup_rejects_retired_single_file_layout() {
        // Sync `#[test]`, so the blocking counterpart. It opens a durable store
        // and therefore needs the encryption env to hold still; it never mutates
        // it, but `blocking_read` is the only guard available off a runtime.
        let _env_read_lock = crate::crypto::TEST_ENV_LOCK.blocking_read();
        let dir = std::env::temp_dir().join(format!(
            "eg-shard-retired-layout-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let retired = Database::create(dir.join("graph.redb")).unwrap();
        drop(retired);

        let err = match RedbBackend::open_with_shards(dir.to_string_lossy().to_string(), 64, 1) {
            Ok(backend) => {
                backend.shutdown();
                panic!("retired layout must require an offline migration");
            }
            Err(err) => err,
        };
        assert!(err.contains("retired redb layout"), "{err}");
        assert!(!dir.join("graph-0.redb").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// K>1 routes a graph's writes to a DETERMINISTIC shard (proven by per-shard
    /// commit stats: only the owning shard commits) and the write round-trips durably
    /// ACROSS A RESTART (reopen + read the node back from disk).
    #[tokio::test(flavor = "multi_thread")]
    async fn k_gt_1_routes_to_deterministic_shard_and_survives_restart() {
        // Held for the whole test: this test opens the backend TWICE (initial write,
        // then a restart reopen) and both `RedbBackend::open_with_shards` calls must
        // resolve the SAME encryption-at-rest cipher (`ValueCipher::from_env_checked`,
        // resolved fresh at each `open`) or the restart reopen's read fails with
        // "encrypted durable value is missing sealed framing" -- the exact
        // destructive-read mismatch documented on `RedbBackend::transaction_recovery_cipher`.
        // This test never sets `EPISTEMIC_GRAPH_ENCRYPTION_KEY` itself; it only needs
        // the AMBIENT value (set or unset) to stay constant across both opens, which
        // requires excluding every other test that mutates that process-global for its
        // duration. See `crate::crypto::acquire_test_env_lock`'s doc.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-shard-route-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        const K: usize = 4;
        // `record_durable`/`read_node`/`shard_index` all take a graph FNAME -- the
        // sanitized storage spelling, as their parameter names and `shard_index`'s
        // own doc say, and as every production caller passes
        // (`server::mutation::commit_finalize_durable` does
        // `crate::persist::sanitize(ctx.graph_name)` first). This test is the only
        // one in the file that picked a name needing sanitization (`:` is outside
        // the storage alphabet) and then skipped the step, so its durable commit
        // was refused by `validate_ordinary_physical_graph_key` with "physical
        // graph key must use the sanitized storage alphabet". Sanitize once and
        // use the fname everywhere the API asks for one -- which keeps the point
        // of the test (a punctuated, namespaced graph name routes deterministically
        // and survives a restart) rather than trading it for an unpunctuated name.
        let graph = "agent:router-test";
        let graph_fname = crate::persist::sanitize(graph);
        let owner = shard_index(&graph_fname, K);

        {
            let backend = RedbBackend::open_with_shards(dir_s.clone(), 256, K).expect("open K=4");
            assert_eq!(backend.shard_count(), K);
            // All K shard files exist (each acquired its exclusive lock at open).
            for i in 0..K {
                assert!(
                    dir.join(format!("graph-{i}.redb")).exists(),
                    "shard {i} file"
                );
            }
            backend
                .record_durable(
                    &graph_fname,
                    &Method::AddNode {
                        node_id: "n1".to_string(),
                        properties_msgpack: props(serde_json::json!({"v": 1})),
                    },
                )
                .await
                .expect("durable commit");
            // Determinism proof: ONLY the routed shard committed.
            let stats = backend.commit_stats_all();
            assert!(stats[owner].commits() > 0, "owning shard {owner} committed");
            for (i, st) in stats.iter().enumerate() {
                if i != owner {
                    assert_eq!(st.commits(), 0, "non-owning shard {i} must not commit");
                }
            }
            assert!(
                backend
                    .read_node(&graph_fname, "n1")
                    .await
                    .unwrap()
                    .is_some(),
                "node readable pre-restart"
            );
            backend.shutdown();
        }

        // RESTART: reopen the SAME dir (K reconciled from on-disk layout) and read the
        // node straight back from disk — durability across a process restart.
        {
            let backend = RedbBackend::open_with_shards(dir_s.clone(), 256, K).expect("reopen K=4");
            assert_eq!(backend.shard_count(), K, "K reconciled from disk");
            assert!(
                backend
                    .read_node(&graph_fname, "n1")
                    .await
                    .unwrap()
                    .is_some(),
                "node survived restart, served from the same shard"
            );
            backend.shutdown();
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Two graphs on DIFFERENT shards commit CONCURRENTLY — the multicore win: K
    /// independent single-writer files commit in parallel. Proven by both writes
    /// succeeding and per-shard stats showing two distinct shards each committed.
    #[tokio::test(flavor = "multi_thread")]
    async fn two_graphs_on_different_shards_commit_concurrently() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-shard-par-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        const K: usize = 4;
        // Find two graph names that route to distinct shards.
        let mut a = String::new();
        let mut b = String::new();
        for i in 0..1000 {
            let name = format!("g{i}");
            let s = shard_index(&name, K);
            if a.is_empty() {
                a = name;
            } else if shard_index(&a, K) != s {
                b = name;
                break;
            }
        }
        assert!(!b.is_empty(), "found two graphs on different shards");
        let (sa, sb) = (shard_index(&a, K), shard_index(&b, K));
        assert_ne!(sa, sb);

        let backend = Arc::new(RedbBackend::open_with_shards(dir_s.clone(), 256, K).expect("open"));
        // Fire both concurrently — they target different writer threads / files.
        let ba = backend.clone();
        let bb = backend.clone();
        let ga = a.clone();
        let gb = b.clone();
        let ha = tokio::spawn(async move {
            for i in 0..50 {
                ba.record_durable(
                    &ga,
                    &Method::AddNode {
                        node_id: format!("a{i}"),
                        properties_msgpack: props(serde_json::json!({"i": i})),
                    },
                )
                .await
                .expect("a durable");
            }
        });
        let hb = tokio::spawn(async move {
            for i in 0..50 {
                bb.record_durable(
                    &gb,
                    &Method::AddNode {
                        node_id: format!("b{i}"),
                        properties_msgpack: props(serde_json::json!({"i": i})),
                    },
                )
                .await
                .expect("b durable");
            }
        });
        ha.await.unwrap();
        hb.await.unwrap();

        let stats = backend.commit_stats_all();
        assert!(stats[sa].commits() > 0, "shard {sa} committed graph A");
        assert!(stats[sb].commits() > 0, "shard {sb} committed graph B");
        assert!(stats[sa].ops() >= 50 && stats[sb].ops() >= 50);
        // Both graphs fully durable.
        assert!(backend.read_node(&a, "a49").await.unwrap().is_some());
        assert!(backend.read_node(&b, "b49").await.unwrap().is_some());
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The `EPISTEMIC_GRAPH_REDB_SHARDS` env override is honored by `open()` even in
    /// cfg(test) (the env check precedes the test default). Serialized vs the other
    /// env-mutating tests via the shared lock.
    #[tokio::test]
    async fn shards_env_override_is_honored() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-shard-env-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = {
            let _env = LINGER_ENV_LOCK.lock().unwrap();
            std::env::set_var("EPISTEMIC_GRAPH_REDB_SHARDS", "3");
            let b = RedbBackend::open(dir_s.clone(), 64).expect("open");
            std::env::remove_var("EPISTEMIC_GRAPH_REDB_SHARDS");
            b
        };
        assert_eq!(backend.shard_count(), 3, "env override sets K=3");
        for i in 0..3 {
            assert!(dir.join(format!("graph-{i}.redb")).exists());
        }
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.sharding.r5-feature (R5) — the catalog auto-attach gate + the empty-catalog routing
    /// identity. A durable `catalog.redb` ⇒ `open()` attaches it; absent (and no flag) ⇒
    /// no catalog (pure EG-026). An EMPTY catalog resolves every graph to the exact EG-026
    /// `shard_index` (byte-for-byte), and an explicit assignment overrides the hash.
    #[tokio::test]
    async fn catalog_auto_attach_gate_and_empty_is_fnv1a() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::server::persistence::tenant_catalog::TenantCatalog;
        let root = std::env::temp_dir().join(format!("eg-r5-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);

        // (1) No catalog.redb, no env ⇒ open() attaches NOTHING (default EG-026 routing).
        let plain_dir = root.join("plain");
        std::fs::create_dir_all(&plain_dir).unwrap();
        let plain = RedbBackend::open(plain_dir.to_string_lossy().to_string(), 64).unwrap();
        assert!(plain.catalog().is_none(), "default open() has no catalog");
        plain.shutdown();

        // (2) A durable catalog.redb present ⇒ open() auto-attaches it, loading its
        // placements. This is the "attach if a durable catalog exists" gate.
        let cat_dir = root.join("cat");
        let cat_dir_s = cat_dir.to_string_lossy().to_string();
        {
            let cat = TenantCatalog::open(&cat_dir_s).expect("seed catalog");
            cat.assign("pinned", 0, None).unwrap();
        }
        let attached = RedbBackend::open(cat_dir_s.clone(), 64).unwrap();
        let cat = attached
            .catalog()
            .expect("durable catalog auto-attached at open");
        assert_eq!(cat.len(), 1, "the prior placement survived + reloaded");
        attached.shutdown();

        // (3) Empty catalog routes IDENTICALLY to EG-026 for every graph/K (no regression).
        let empty = TenantCatalog::in_memory();
        for k in [1usize, 2, 4, 8, 16] {
            for g in ["__commons__", "agent:7", "g-move", "tenant_x", "ZZZ"] {
                assert_eq!(
                    empty.resolve_shard(g, k),
                    shard_index(g, k),
                    "empty catalog == FNV-1a for g={g} K={k}"
                );
            }
        }
        // An explicit override wins (the routing flip an online reshard performs).
        empty.assign("g-move", 0, None).unwrap();
        assert_eq!(empty.resolve_shard("g-move", 4), 0);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// CONCEPT:EG-KG.backend.catalog-shard-resolve (R1) — move ONE graph between shards while the engine RUNS, with
    /// concurrent writes to that graph ACROSS the flip. Every node/edge survives, the
    /// audit chain stays valid, reads/writes follow the graph to its new shard, the source
    /// rows are GC'd, and an unrelated graph is untouched.
    #[tokio::test(flavor = "multi_thread")]
    async fn online_reshard_moves_graph_live_no_loss() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::server::persistence::tenant_catalog::TenantCatalog;
        const K: usize = 4;
        let dir = std::env::temp_dir().join(format!("eg-r1-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let dir_s = dir.to_string_lossy().to_string();

        let catalog = Arc::new(TenantCatalog::open(&dir_s).expect("catalog"));
        let backend = Arc::new(
            RedbBackend::open_with_shards(dir_s.clone(), 256, K)
                .expect("open K=4")
                .with_catalog(catalog.clone()),
        );
        assert_eq!(backend.shard_count(), K);

        let mover = "g-move";
        let stay = "g-stay";
        let src = shard_index(mover, K);
        let dst = (src + 1) % K;

        for g in [mover, stay] {
            backend
                .register_graph(g, g, GraphType::Global)
                .await
                .unwrap();
        }
        for i in 0..10u32 {
            backend
                .record_durable(
                    mover,
                    &Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: props(serde_json::json!({"g": mover, "i": i})),
                    },
                )
                .await
                .unwrap();
        }
        backend
            .record_durable(
                mover,
                &Method::AddEdge {
                    source_id: "n0".into(),
                    target_id: "n1".into(),
                    properties_msgpack: props(serde_json::json!({"w": 1})),
                },
            )
            .await
            .unwrap();
        for i in 0..5u32 {
            backend
                .record_durable(
                    stay,
                    &Method::AddNode {
                        node_id: format!("s{i}"),
                        properties_msgpack: props(serde_json::json!({"g": stay, "i": i})),
                    },
                )
                .await
                .unwrap();
        }
        assert_eq!(
            catalog.resolve_shard(mover, K),
            src,
            "FNV route before any move"
        );

        // Concurrent writes to `mover` fired together with the move — they straddle the
        // route flip and must ALL survive on the destination shard (zero lost/misrouted).
        let writer_backend = backend.clone();
        let writer = tokio::spawn(async move {
            for i in 10..20u32 {
                writer_backend
                    .record_durable(
                        "g-move",
                        &Method::AddNode {
                            node_id: format!("n{i}"),
                            properties_msgpack: props(serde_json::json!({"g": "g-move", "i": i})),
                        },
                    )
                    .await
                    .expect("concurrent durable");
            }
        });
        let report = backend
            .reshard_graph(mover, dst as u32)
            .await
            .expect("reshard");
        writer.await.unwrap();

        assert!(!report.no_op);
        assert_eq!((report.from_shard, report.to_shard), (src, dst));
        assert_eq!(
            catalog.resolve_shard(mover, K),
            dst,
            "route now follows to dst"
        );

        // All 20 nodes + the edge survive and read back from the NEW shard.
        let dump = backend
            .read_graph_dump_blocking(mover)
            .unwrap()
            .expect("mover present after move");
        assert_eq!(dump.nodes.len(), 20, "pre + concurrent nodes all survived");
        assert_eq!(dump.edges.len(), 1, "edge survived");
        for i in 0..20u32 {
            assert!(
                dump.nodes.iter().any(|(id, _)| id == &format!("n{i}")),
                "node n{i} present after move"
            );
        }

        // A post-move write lands on the NEW shard (route followed the graph).
        backend
            .record_durable(
                mover,
                &Method::AddNode {
                    node_id: "post".into(),
                    properties_msgpack: props(serde_json::json!({"g": "g-move"})),
                },
            )
            .await
            .unwrap();
        let dump2 = backend.read_graph_dump_blocking(mover).unwrap().unwrap();
        assert_eq!(dump2.nodes.len(), 21, "post-move write on the new shard");

        // The unrelated graph is completely unaffected.
        let sdump = backend
            .read_graph_dump_blocking(stay)
            .unwrap()
            .expect("stay present");
        assert_eq!(sdump.nodes.len(), 5);

        // The tamper-evident audit chain still verifies on the new shard.
        #[cfg(feature = "security")]
        {
            let audit = backend.audit_verify_blocking(mover).expect("audit verify");
            assert!(
                audit.ok,
                "audit chain valid after the online move: {}",
                audit.detail
            );
        }

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.sharding.eg-r6 (R6) — a cold (idle) graph is offloaded: its whole in-RAM state is
    /// dropped to bound RAM, yet every node still SERVES on access via the KG-2.191
    /// read-through from redb. The shared `__commons__` is never offloaded.
    #[tokio::test(flavor = "multi_thread")]
    async fn cold_offload_evicts_then_serves_on_access() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::server::persistence::cold_offload::{offload_cold_tenants, ColdTenantTracker};
        let dir = std::env::temp_dir().join(format!("eg-r6-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), 256).expect("open"));
        let state = new_state(Some(dir_s.clone()));

        let core = seed_authoritative(&backend, &state, 12).await;
        assert_eq!(core.node_count(), 12, "all nodes resident before offload");

        let tracker = ColdTenantTracker::new();
        tracker.touch("g1");
        // Window 0 ⇒ g1 is cold ⇒ offloaded; __commons__ is skipped.
        let offloaded = offload_cold_tenants(&state, &tracker, std::time::Duration::ZERO).await;
        assert_eq!(offloaded, 1, "exactly g1 offloaded");
        assert!(tracker.is_offloaded("g1"));
        assert_eq!(tracker.offloaded_total(), 1);
        assert_eq!(core.node_count(), 0, "in-RAM state evicted by offload");

        // Served on access: every node reads back from redb via the read-through seam.
        for i in 0..12usize {
            assert_eq!(
                core.get_node_properties(&format!("n{i}")),
                Some(props(serde_json::json!({"type": "Task", "i": i}))),
                "offloaded node n{i} serves from redb on access"
            );
        }

        // Re-touch clears the offload mark + resets the idle clock (windowing).
        tracker.touch("g1");
        assert!(!tracker.is_offloaded("g1"));
        assert!(
            tracker
                .cold_graphs(std::time::Duration::from_secs(3600))
                .is_empty(),
            "a just-touched graph is not cold under a long window"
        );

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:AU-KG.backend.roadmap-f-parallel-cross (roadmap F) — the per-shard read fan-out runs CONCURRENTLY, not
    /// serially. A `Barrier(K)` only releases once all K closures are running at the SAME
    /// time; a serial spawn-then-await-each impl would block forever, which the timeout
    /// converts into a test failure. Results come back in shard order.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fan_out_shard_reads_runs_concurrently() {
        use std::sync::Barrier;
        const K: usize = 4;
        let barrier = Arc::new(Barrier::new(K));
        let tasks: Vec<_> = (0..K)
            .map(|i| {
                let b = barrier.clone();
                move || -> Result<usize, String> {
                    // `Barrier::wait` is a `disallowed_methods` entry because an
                    // unbounded wait hangs instead of failing. Here the bound
                    // already exists, and in a better place: the whole fan-out
                    // is driven under the `tokio::time::timeout(10s)` below,
                    // which this test's doc comment names as the mechanism that
                    // "converts into a test failure" the serial-impl regression
                    // this barrier exists to detect. A second deadline inside
                    // the closure could only fire after the outer one already
                    // failed the test. Invariant `enclosing-deadline`:
                    // docs/architecture/liveness_invariants.md.
                    #[allow(clippy::disallowed_methods)]
                    b.wait();
                    Ok(i)
                }
            })
            .collect();
        let out = tokio::time::timeout(Duration::from_secs(10), join_blocking_in_order(tasks))
            .await
            .expect("fan-out ran concurrently (a serial impl would deadlock on the barrier)")
            .expect("all shard reads ok");
        assert_eq!(out, vec![0, 1, 2, 3], "results returned in shard order");
    }

    /// CONCEPT:AU-KG.backend.roadmap-f-parallel-cross (roadmap F) — `load_all` fans each shard's dump CONCURRENTLY off a
    /// `begin_read()` snapshot (off the writer) and unions them. Seed graphs spread across
    /// K=4 shards, commit, drop, reopen, load → every graph is recovered from its shard.
    #[tokio::test(flavor = "multi_thread")]
    async fn parallel_load_recovers_all_shards_off_the_writer() {
        // Held for the whole test: same requirement as
        // `k_gt_1_routes_to_deterministic_shard_and_survives_restart` above -- this
        // test opens the backend TWICE (initial write, then a restart reopen after
        // `shutdown()`/drop) and both opens must resolve the same encryption-at-rest
        // cipher or the reload's read fails with "encrypted durable value is missing
        // sealed framing". Never sets the key itself; only needs the ambient value to
        // stay constant across both opens. See `crate::crypto::acquire_test_env_lock`'s
        // doc.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        const K: usize = 4;
        let dir = std::env::temp_dir().join(format!("eg-f-load-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let names = [
            "alpha", "beta", "gamma", "delta", "eps", "zeta", "eta", "theta",
        ];

        let backend = RedbBackend::open_with_shards(dir_s.clone(), 64, K).expect("open K=4");
        let state = new_state(Some(dir_s.clone()));
        {
            let mut s = state.write().await;
            for n in names {
                let _ = s.registry.create_graph(n, GraphType::Global, None);
            }
        }
        for n in names {
            backend
                .register_graph(n, n, GraphType::Global)
                .await
                .unwrap();
            let core = {
                let s = state.read().await;
                s.registry.get(n).map(|e| e.core.clone()).unwrap()
            };
            core.add_node("x".into(), props(serde_json::json!({"g": n})));
            backend
                .record_durable(
                    n,
                    &Method::AddNode {
                        node_id: "x".into(),
                        properties_msgpack: props(serde_json::json!({"g": n})),
                    },
                )
                .await
                .unwrap();
        }
        // The seed graphs must span >1 shard, else the parallel union proves nothing.
        let used: std::collections::HashSet<usize> = names
            .iter()
            .map(|n| shard_index(&crate::persist::sanitize(n), K))
            .collect();
        assert!(used.len() >= 2, "seed graphs span multiple shards");
        backend.shutdown();
        // `shutdown()` stops the writer threads but does NOT close each shard's
        // `Database`; redb holds its advisory file lock for as long as any
        // `Arc<RedbBackend>` lives, so an in-process reopen of the same directory
        // fails with "Database already open. Cannot acquire lock." unless every
        // reference is actually released first.
        {
            let mut s = state.write().await;
            s.persistence = None;
        }
        drop(backend);

        let backend2 = RedbBackend::open_with_shards(dir_s.clone(), 64, K).expect("reopen K=4");
        let state2 = new_state(Some(dir_s.clone()));
        let loaded = backend2.load_all(&state2).await.unwrap();
        assert!(loaded >= names.len(), "all seeded graphs recovered");
        for n in names {
            let core = {
                let s = state2.read().await;
                s.registry
                    .get(n)
                    .map(|e| e.core.clone())
                    .expect("graph recovered")
            };
            assert_eq!(
                core.get_node_properties("x"),
                Some(props(serde_json::json!({"g": n}))),
                "graph {n} recovered from its shard via the parallel fan-out"
            );
        }
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.r3-plan-execution (R3 plan execution) — a fully-skewed placement (every graph pinned to
    /// shard 0) → `plan_rebalance` → `rebalance_execute` applies each move via online
    /// resharding → graphs spread across shards and every node survives (no loss).
    #[tokio::test(flavor = "multi_thread")]
    async fn rebalance_execute_balances_and_preserves_data() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::server::persistence::rebalance::{
            plan_rebalance, shard_loads_from_catalog, RebalanceOptions,
        };
        use crate::server::persistence::tenant_catalog::TenantCatalog;
        const K: usize = 4;
        let dir = std::env::temp_dir().join(format!("eg-r3-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let catalog = Arc::new(TenantCatalog::open(&dir_s).expect("catalog"));
        let backend = RedbBackend::open_with_shards(dir_s.clone(), 256, K)
            .expect("open K=4")
            .with_catalog(catalog.clone());

        let names = ["g0", "g1", "g2", "g3", "g4", "g5"];
        for n in names {
            catalog.assign(n, 0, None).unwrap(); // pin ALL to shard 0 (skew)
            backend
                .register_graph(n, n, GraphType::Global)
                .await
                .unwrap();
            backend
                .record_durable(
                    n,
                    &Method::AddNode {
                        node_id: "a".into(),
                        properties_msgpack: props(serde_json::json!({"g": n})),
                    },
                )
                .await
                .unwrap();
        }
        assert!(
            names.iter().all(|n| catalog.resolve_shard(n, K) == 0),
            "fully skewed onto shard 0 before rebalance"
        );

        let loads: Vec<(String, u64)> = names.iter().map(|n| (n.to_string(), 1u64)).collect();
        let shards = shard_loads_from_catalog(&catalog, &loads, K);
        let plan = plan_rebalance(&shards, RebalanceOptions::default());
        assert!(!plan.is_empty(), "a fully-skewed set yields moves");
        let reports = backend.rebalance_execute(&plan).await.expect("execute");
        assert_eq!(reports.len(), plan.moves.len(), "one report per move");

        let distinct: std::collections::HashSet<usize> =
            names.iter().map(|n| catalog.resolve_shard(n, K)).collect();
        assert!(
            distinct.len() >= 2,
            "graphs spread across >1 shard after rebalance, got {distinct:?}"
        );
        for n in names {
            assert!(
                backend.read_node_blocking(n, "a").unwrap().is_some(),
                "graph {n} node survives the rebalance move"
            );
        }
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.flush-pending-first (R1 delta-copy) — moving an IDLE graph copies the whole graph in the
    /// UNQUIESCED bulk pass, so the under-quiesce DELTA (the work that actually pauses the
    /// moved graph's writes) is 0. Proves the snapshot+delta path shrank the pause to ~0
    /// for the common idle case, with no data loss.
    #[tokio::test(flavor = "multi_thread")]
    async fn online_reshard_delta_is_small_for_idle_graph() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::server::persistence::tenant_catalog::TenantCatalog;
        const K: usize = 4;
        let dir = std::env::temp_dir().join(format!("eg-r1-delta-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let catalog = Arc::new(TenantCatalog::open(&dir_s).expect("catalog"));
        let backend = RedbBackend::open_with_shards(dir_s.clone(), 256, K)
            .expect("open")
            .with_catalog(catalog.clone());

        let g = "idle-graph";
        backend
            .register_graph(g, g, GraphType::Global)
            .await
            .unwrap();
        for i in 0..20u32 {
            backend
                .record_durable(
                    g,
                    &Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: props(serde_json::json!({"i": i})),
                    },
                )
                .await
                .unwrap();
        }
        let src = catalog.resolve_shard(g, K);
        let dst = (src + 1) % K;
        let report = backend.reshard_graph(g, dst as u32).await.expect("reshard");
        assert!(!report.no_op);
        assert!(
            report.nodes >= 20,
            "bulk pass copied the whole graph ({})",
            report.nodes
        );
        assert_eq!(
            report.delta_nodes, 0,
            "idle graph ⇒ zero under-quiesce node copy"
        );
        assert_eq!(
            report.delta_edges, 0,
            "idle graph ⇒ zero under-quiesce edge copy"
        );
        for i in 0..20u32 {
            assert!(
                backend
                    .read_node_blocking(g, &format!("n{i}"))
                    .unwrap()
                    .is_some(),
                "node n{i} survives the move"
            );
        }
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// WD5-BUG-04 (online-reshard half of the SAME defect class WD3-BUG-01 fixed
    /// offline in `shard_migrate.rs::migrate_shards`, see its commit's coverage
    /// matrix): a representative row from RESOURCE_*, development_lane_*,
    /// capacity_lease_*, and (under `security`) PROVENANCE_ANCHOR_MEMBERS, plus a
    /// WORK_ITEM_COMMAND_SEQUENCE row, is seeded directly (raw redb — these
    /// subsystems require heavy native-operation preconditions a raw seed
    /// sidesteps) into the graph's SOURCE shard; the graph is then resharded onto a
    /// different shard via the public `reshard_graph` API, and every row is
    /// confirmed present on the DESTINATION shard afterward — and (except
    /// `PROVENANCE_ANCHOR_MEMBERS`; see below) absent from the source, proving a
    /// MOVE, not a copy. Deliberately does NOT seed `PLAN_MATVIEWS`/
    /// `MATVIEW_OPERATOR_STATE` (GLOBAL, no graph key — moving them per-graph would
    /// be its own corruption) or the `work_item_capability` trio (deliberately
    /// purged-not-copied by this module's own design). Confirmed FAILING before
    /// this fix — every `table_row_count(&dst_path, ...)` below was 0 against the
    /// unmodified `export_graph_raw`/`import_graph_raw`.
    #[tokio::test(flavor = "multi_thread")]
    async fn online_reshard_preserves_resource_lane_capacity_and_provenance_tables() {
        // WD5-BUG-05: unlike the sibling reshard tests, this one raw-seeds
        // RESOURCE_RESERVATIONS/development_lane::HOLDS with PLAINTEXT msgpack
        // (`seed_valid_resource_reservation`/`seed_valid_development_lane_hold`
        // below) and later reads them back through the backend's OWN decode path
        // (PurgeGraph's pre-existing lifecycle-authority validation — see those
        // helpers' doc comments). `cm_dir` (used by the crossmodal txn tests
        // above) provisions `EPISTEMIC_GRAPH_ENCRYPTION_KEY` PROCESS-GLOBALLY via
        // a `std::sync::Once` and, by its own documented design, NEVER unsets it.
        // A plain READ lock does not protect against that already-set ambient
        // state left behind by an earlier test in the same binary (confirmed:
        // this test fails with "encrypted durable value is missing sealed
        // framing" when run after `crossmodal_txn_commits_all_modalities_atomically`
        // / `crossmodal_measurement_visible_via_public_tsrange_post_commit_and_restart`,
        // but passes standalone). Take the WRITE lock and force encryption OFF for
        // this test's whole body instead, so the seeded plaintext always
        // round-trips regardless of test execution order/composition.
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        #[cfg(feature = "security")]
        let _enc_guard = EncryptionRequiredEnvGuard::set(None, "off");
        #[cfg(feature = "security")]
        use crate::redb_store::PROVENANCE_ANCHOR_MEMBERS;
        use crate::redb_store::{capacity_lease, development_lane};
        use crate::redb_store::{RESOURCE_RESERVATIONS, WORK_ITEM_COMMAND_SEQUENCE};
        use crate::server::persistence::tenant_catalog::TenantCatalog;
        const K: usize = 4;
        let dir = std::env::temp_dir().join(format!("eg-reshard-cx054-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let catalog = Arc::new(TenantCatalog::open(&dir_s).expect("catalog"));

        let g = "cx054-graph";
        // Register the graph THROUGH a live backend so GRAPH_META exists, then shut
        // it down to raw-seed the WD5-BUG-04 tables — the same "open, register,
        // shutdown, raw-seed" sequence `shard_migrate.rs`'s own coverage test uses.
        {
            let backend = RedbBackend::open_with_shards(dir_s.clone(), 256, K)
                .expect("open")
                .with_catalog(catalog.clone());
            backend
                .register_graph(g, g, GraphType::Global)
                .await
                .unwrap();
            backend.shutdown();
        }

        let src_idx = catalog.resolve_shard(g, K);
        let dst_idx = (src_idx + 1) % K;
        let src_path = dir.join(shard_filename(src_idx));

        // RESOURCE_RESERVATIONS and development_lane::HOLDS must be VALID, purge-safe
        // records (not raw junk bytes): unlike backup/restore, online reshard's
        // PurgeGraph step decodes+validates these two tables as a pre-existing
        // lifecycle-authority guard (see the two helpers' doc comments).
        seed_valid_resource_reservation(&src_path, g, "r1", "cx054-tenant");
        seed_valid_development_lane_hold(&src_path, g, "h1", "cx054-tenant");
        seed_raw_row(&src_path, capacity_lease::CELLS, (g, "c1"), b"cell");
        seed_raw_row(&src_path, WORK_ITEM_COMMAND_SEQUENCE, g, 7u64);
        #[cfg(feature = "security")]
        seed_raw_row(
            &src_path,
            PROVENANCE_ANCHOR_MEMBERS,
            (g, 1u64),
            b"anchor-member",
        );

        // Sanity: every seeded row landed in the source before the reshard.
        assert_eq!(table_row_count(&src_path, RESOURCE_RESERVATIONS), 1);
        assert_eq!(table_row_count(&src_path, development_lane::HOLDS), 1);
        assert_eq!(table_row_count(&src_path, capacity_lease::CELLS), 1);
        assert_eq!(table_row_count(&src_path, WORK_ITEM_COMMAND_SEQUENCE), 1);
        #[cfg(feature = "security")]
        assert_eq!(table_row_count(&src_path, PROVENANCE_ANCHOR_MEMBERS), 1);

        let backend = RedbBackend::open_with_shards(dir_s.clone(), 256, K)
            .expect("reopen")
            .with_catalog(catalog.clone());
        let report = backend
            .reshard_graph(g, dst_idx as u32)
            .await
            .expect("reshard");
        assert!(!report.no_op);
        assert!(
            report.capability_and_resource >= 4,
            "WD5-BUG-04 tables counted in the reshard report ({})",
            report.capability_and_resource
        );
        backend.shutdown();

        let dst_path = dir.join(shard_filename(dst_idx));
        assert_eq!(
            table_row_count(&dst_path, RESOURCE_RESERVATIONS),
            1,
            "resource_reservations survives the online reshard (WD5-BUG-04)"
        );
        assert_eq!(
            table_row_count(&dst_path, development_lane::HOLDS),
            1,
            "development_lane_holds survives the online reshard (WD5-BUG-04)"
        );
        assert_eq!(
            table_row_count(&dst_path, capacity_lease::CELLS),
            1,
            "capacity_cells survives the online reshard (WD5-BUG-04)"
        );
        assert_eq!(
            table_row_count(&dst_path, WORK_ITEM_COMMAND_SEQUENCE),
            1,
            "work_item_command_sequence survives the online reshard (WD5-BUG-04)"
        );
        #[cfg(feature = "security")]
        assert_eq!(
            table_row_count(&dst_path, PROVENANCE_ANCHOR_MEMBERS),
            1,
            "provenance_anchor_members survives the online reshard (WD5-BUG-04)"
        );

        // The move, not a copy: `Cmd::PurgeGraph`'s `purge_graph_rows` (step 4 of
        // `delta_flip_purge`) already unconditionally clears RESOURCE_*/
        // development_lane_*/capacity_lease_*/WORK_ITEM_COMMAND_SEQUENCE for the
        // graph at the source — that call was ALREADY correct before this fix and is
        // exactly why the pre-fix bug was true data loss (drop from source, no copy
        // to destination), not a leak. `PROVENANCE_ANCHOR_MEMBERS` is the one
        // exception: `purge_graph_rows` does not clear it (a pre-existing gap in
        // `redb_store.rs`, outside this lane's two-file scope — see the lane
        // report), so its source-side row is orphaned rather than purged.
        assert_eq!(
            table_row_count(&src_path, RESOURCE_RESERVATIONS),
            0,
            "resource_reservations purged from the source after the move"
        );
        assert_eq!(
            table_row_count(&src_path, development_lane::HOLDS),
            0,
            "development_lane_holds purged from the source after the move"
        );
        assert_eq!(
            table_row_count(&src_path, capacity_lease::CELLS),
            0,
            "capacity_cells purged from the source after the move"
        );
        assert_eq!(
            table_row_count(&src_path, WORK_ITEM_COMMAND_SEQUENCE),
            0,
            "work_item_command_sequence purged from the source after the move"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.m3-admin-dispatch — drive the M3 admin ops over the FULL dispatch path (protocol →
    /// `handlers::admin` → the persistence APIs): assign a catalog placement, list it back,
    /// and get a rebalance plan. Proves the WIRE surface, not just the backend methods.
    #[tokio::test(flavor = "multi_thread")]
    async fn admin_rpc_dispatch_roundtrip() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::protocol::ResultPayload;
        use crate::server::persistence::tenant_catalog::TenantCatalog;
        const SECRET: &str = "admin-rpc";
        const K: usize = 4;
        let dir = std::env::temp_dir().join(format!("eg-admin-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let catalog = Arc::new(TenantCatalog::open(&dir_s).expect("catalog"));
        let backend: Arc<dyn PersistenceBackend> = Arc::new(
            RedbBackend::open_with_shards(dir_s.clone(), 256, K)
                .expect("open")
                .with_catalog(catalog.clone()),
        );
        let state = new_state(Some(dir_s.clone()));
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            s.persistence = Some(backend.clone());
        }
        let req = |id: u64, method: Method| current_request(SECRET, id, "__commons__", method);

        // CatalogAssign → Bool(true).
        let r = dispatch_on_heap(
            &state,
            req(
                1,
                Method::CatalogAssign {
                    graph: "g".into(),
                    shard: 2,
                    node: None,
                },
            ),
        )
        .await;
        assert!(
            matches!(r.result, Some(ResultPayload::Bool(true))),
            "assign ok: {:?}",
            r.error
        );

        // CatalogList → JSON containing the placement we just wrote.
        let r = dispatch_on_heap(&state, req(2, Method::CatalogList)).await;
        match r.result {
            Some(ResultPayload::Json(v)) => {
                let placements = v
                    .get("placements")
                    .and_then(|p| p.as_array())
                    .cloned()
                    .unwrap_or_default();
                assert!(
                    placements.iter().any(|p| {
                        p.get("graph").and_then(|g| g.as_str()) == Some("g")
                            && p.get("shard").and_then(|s| s.as_u64()) == Some(2)
                    }),
                    "placement present in list: {placements:?}"
                );
            }
            other => panic!("CatalogList json, got {other:?}"),
        }

        // RebalancePlan → JSON with `moves` + `shards` arrays (read-only).
        let r = dispatch_on_heap(
            &state,
            req(
                3,
                Method::RebalancePlan {
                    tolerance: None,
                    max_moves: None,
                },
            ),
        )
        .await;
        match r.result {
            Some(ResultPayload::Json(v)) => {
                assert!(v.get("moves").map(|m| m.is_array()).unwrap_or(false));
                assert!(v.get("shards").map(|m| m.is_array()).unwrap_or(false));
            }
            other => panic!("RebalancePlan json, got {other:?}"),
        }

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.r6-feature (R6 touch wiring) — the cold sweep selects a graph IDLE past the
    /// window but NEVER a recently-touched one. Proves the touch-driven selection semantics
    /// the dispatch read/write path relies on.
    #[test]
    fn touch_keeps_accessed_graph_resident_idle_is_cold() {
        use crate::server::persistence::cold_offload::ColdTenantTracker;
        let tracker = ColdTenantTracker::new();
        let window = Duration::from_millis(40);
        tracker.touch("hot");
        tracker.touch("cold");
        std::thread::sleep(Duration::from_millis(70)); // both idle past the window
        tracker.touch("hot"); // re-access "hot"
        let cold = tracker.cold_graphs(window);
        assert!(
            cold.contains(&"cold".to_string()),
            "an idle graph is a cold candidate"
        );
        assert!(
            !cold.contains(&"hot".to_string()),
            "a recently-touched graph stays resident"
        );
    }
}
